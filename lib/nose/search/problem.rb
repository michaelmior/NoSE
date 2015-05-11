require 'logging'

begin
  require 'gurobi'
rescue LoadError
  # We can't use most search functionality, but it won't explode
  nil
end

module NoSE::Search
  # A representation of a search problem as an ILP
  class Problem
    attr_reader :model, :status, :queries, :updates, :index_vars, :query_vars,
                :indexes, :data

    def initialize(queries, updates, indexes, data)
      @queries = queries
      @updates = updates
      @indexes = indexes
      @data = data
      @logger = Logging.logger['nose::search::problem']
      @status = nil

      setup_model
    end

    # Run the solver and make the selected indexes available
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

    # Build the ILP by creating all the variables and constraints
    def setup_model
      # Set up solver environment
      env = Gurobi::Env.new
      env.set_int Gurobi::IntParam::METHOD,
                  Gurobi::METHOD_DETERMINISTIC_CONCURRENT

      # This copies the number of parallel processes used by the parallel gem
      # so it implicitly respects the --no-parallel CLI flag
      env.set_int Gurobi::IntParam::THREADS, Parallel.processor_count

      # Disable console output since it's quite messy
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

    # Deal with updates which do not require support queries
    def add_update_costs(min_cost, data, update_divisors)
      @updates.each do |update|
        @indexes.each do |index|
          next if !update.modifies_index?(index) ||
                  update_divisors[update].include?(index)

          min_cost += Gurobi::LinExpr.new \
                      update_cost(update, index, 1, data[:cost_model])
        end
      end
    end

    # Get the total cost of the query for the objective function
    def total_query_cost(query, cost, query_var, data, update_divisors)
      return if cost.nil?
      query_cost = cost.last * 1.0

      if query.is_a? NoSE::SupportQuery
        # Add the cost of inserting or deleting data divided by
        # the number of required support queries to avoid double counting
        query_cost += update_cost(query, query.index,
                                  data[:cardinality][query],
                                  data[:cost_model]) /
                      update_divisors[query.statement].count
      end

      query_var * query_cost
    end

    # Set the value of the objective function (workload cost)
    def set_objective
      # Get divisors whih we later use for support queries
      update_divisors = Hash.new { |h, k| h[k] = Set.new }
      @queries.map do |query|
        next unless query.is_a? NoSE::SupportQuery
        update_divisors[query.statement].add query.index
      end

      min_cost = (0...@query_vars.length).to_a \
        .product((0...@queries.length).to_a).map do |i, q|
        total_query_cost @queries[q], @data[:costs][q][i],
                         @query_vars[i][q], data, update_divisors
      end.compact.reduce(&:+)

      add_update_costs min_cost, data, update_divisors

      @logger.debug { "Objective function is #{min_cost.inspect}" }

      @model.setObjective min_cost, Gurobi::MINIMIZE
    end

    # Get the cost of an index update for a given number of entities
    def update_cost(statement, index, cardinality, cost_model)
      cost = 0
      state = NoSE::Plans::StatementState.new statement, nil
      state.cardinality = cardinality

      if statement.requires_delete?
        step = NoSE::Plans::DeletePlanStep.new index, state
        cost += step.cost cost_model
      end
      if statement.requires_insert?
        step = NoSE::Plans::InsertPlanStep.new index, state
        cost += step.cost cost_model
      end

      cost
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
