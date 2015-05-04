require 'logging'

begin
  require 'gurobi'
rescue LoadError
  # We can't use most search functionality, but it won't explode
  nil
end

module NoSE::Search
  class Problem
    attr_reader :model, :status, :queries, :index_vars, :query_vars,
                :indexes, :data

    def initialize(queries, indexes, data)
      @queries = queries
      @indexes = indexes
      @data = data
      @logger = Logging.logger['nose::search::problem']
      @status = nil

      setup_model
    end

    def solve
      return unless @status.nil?

      # Run the optimization
      @model.optimize

      # Ensure we found a valid solution
      @status = model.get_int(Gurobi::IntAttr::STATUS)
      if @status == Gurobi::INFEASIBLE
        @model.computeIIS
        log_model 'IIS', '.ilp'
      end

      fail NoSolutionException if @status != Gurobi::OPTIMAL
    end

    # Return the value of the objective function if the problem is solved
    def objective_value
      return unless @status == Gurobi::OPTIMAL
      @model.get_double Gurobi::DoubleAttr::OBJ_VAL
    end

    # Return the selected indices
    def selected_indexes
      return if @status.nil?

      @indexes.select.with_index do |_, i|
        # Even though we specifed the variables as binary, rounding
        # error means the value won't be exactly one
        # (the check exists to catch weird values if they arise)
        val = @index_vars[i].get_double Gurobi::DoubleAttr::X
        fail if val > 0.01 && val < 0.99
        val > 0.99
      end
    end

    private

    # Write a model to a temporary file and log the file name
    def log_model(type, extension)
      @logger.debug do
        tmpfile = Tempfile.new ['model', extension]
        ObjectSpace.undefine_finalizer tmpfile
        @model.write tmpfile.path
        "#{type} written to #{tmpfile.path}"
      end
    end

    def setup_model
      # Set up solver environment
      env = Gurobi::Env.new
      env.set_int Gurobi::IntParam::METHOD,
                  Gurobi::METHOD_DETERMINISTIC_CONCURRENT
      env.set_int Gurobi::IntParam::THREADS, Parallel.processor_count
      env.set_int Gurobi::IntParam::OUTPUT_FLAG, 0

      @model = Gurobi::Model.new env
      add_variables
      @model.update
      add_constraints
      set_objective
      @model.update

      log_model 'Model', '.lp'
    end

    # Initialize query and index variables
    def add_variables
      @index_vars = []
      @query_vars = []
      (0...@indexes.length).each do |i|
        @index_vars[i] = @model.addVar 0, 1, 0, Gurobi::BINARY, "i#{i}"
        @query_vars[i] = []
        (0...@queries.length).each do |q|
          @query_vars[i][q] = @model.addVar 0, 1, 0, Gurobi::BINARY,
                                            "q#{q}i#{i}"
        end
      end
    end

    # Add all necessary constraints to the model
    def add_constraints
      [
        IndexPresenceConstraints,
        SpaceConstraint,
        CompletePlanConstraints
      ].each { |constraint| constraint.apply self }

      @logger.debug { "Added #{@model.getConstrs.count} constraints to model" }
    end

    # Set the value of the objective function (workload cost)
    def set_objective
      # Get divisors whih we later use for support queries
      query_divisors = Hash.new { |h, k| h[k] = Gurobi::LinExpr.new }
      update_divisors = Hash.new 0
      @queries.map do |query|
        next unless query.is_a? NoSE::SupportQuery
        index_i = @indexes.index(query.index)
        query_divisors[query.statement] += @index_vars[index_i]
        update_divisors[query.statement] += 1
      end

      min_cost = (0...@query_vars.length).to_a \
        .product((0...@queries.length).to_a).map do |i, q|
        next if @data[:costs][q][i].nil?

        query = @queries[q]
        if query.is_a? NoSE::SupportQuery
          query_cost = @data[:costs][q][i].last * 1.0

          # Add the cost of inserting or deleting data divided by
          # the number of required support queries to avoid double counting
          state = QueryState.new query, nil
          state.cardinality = data[:cardinality][query]

          unless query.statement.is_a? NoSE::Insert
            query_cost += DeletePlanStep.new(query.index, state).cost \
              data[:cost_model] / update_divisors[query.statement]
          end
          unless query.statement.is_a? NoSE::Delete
            query_cost += InsertPlanStep.new(query.index, state).cost \
              data[:cost_model] / update_divisors[query.statement]
          end

          # Only count cost if query is necessary (index is present)
          index_i = @indexes.index(query.index)
          query_cost *= @index_vars[index_i]

          # Divide cost by number of support queries used
          query_cost *= @query_vars[i][q] / query_divisors[query.statement]
        else
          query_cost = @query_vars[i][q] * (@data[:costs][q][i].last * 1.0)
        end

        query_cost
      end.compact.reduce(&:+)

      @logger.info { "Objective function is #{min_cost.inspect}" }

      @model.setObjective min_cost, Gurobi::MINIMIZE
    end
  end

  # Thrown when no solution can be found to the ILP
  class NoSolutionException < StandardError
  end
end

# Only do this if Gurobi is available
if Object.const_defined? 'Gurobi'
  Gurobi::LinExpr.class_eval do
    def inspect
      0.upto(size - 1).map do |i|
        coeff = getCoeff i
        var = getVar(i).get_string Gurobi::StringAttr::VAR_NAME
        "#{coeff} * #{var}"
      end.join ' + '
    end
  end
end
