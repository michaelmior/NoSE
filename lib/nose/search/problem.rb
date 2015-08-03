require 'logging'

begin
  require 'gurobi'
rescue LoadError
  # We can't use most search functionality, but it won't explode
  nil
end

module NoSE
  module Search
    # Simple enum for possible objective functions
    module Objective
      # Minimize the cost of statements in the workload
      COST  = 1

      # Minimize the space usage of generated indexes
      SPACE = 2

      # Minimize the total number of indexes
      INDEXES = 3
    end

    # A representation of a search problem as an ILP
    class Problem
      attr_reader :model, :status, :queries, :updates,
                  :index_vars, :query_vars, :indexes, :data,
                  :objective_type, :objective_value

      def initialize(queries, updates, indexes, data,
                     objective = Objective::COST)
        @queries = queries
        @updates = updates
        @indexes = indexes
        @data = data
        @logger = Logging.logger['nose::search::problem']
        @status = nil
        @objective_type = objective

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
        elsif @objective_type != Objective::INDEXES
          @objective_value = @model.get_double Gurobi::DoubleAttr::OBJ_VAL

          # Pin the objective value and optimize again to minimize index usage
          @obj_var.set_double Gurobi::DoubleAttr::UB, @objective_value
          @obj_var.set_double Gurobi::DoubleAttr::LB, @objective_value
          @objective_type = Objective::INDEXES
          define_objective 'objective_indexes'

          @status = nil
          solve
          return
        elsif @objective_value.nil?
          @objective_value = @model.get_double Gurobi::DoubleAttr::OBJ_VAL
        end

        @logger.debug do
          "Final objective value is #{@objective.active.inspect}" \
            " = #{@objective_value}"
        end

        fail NoSolutionException if @status != Gurobi::OPTIMAL
      end

      # Return the selected indices
      def selected_indexes
        return if @status.nil?
        return @selected_indexes if @selected_indexes

        @selected_indexes = @indexes.select do |index|
          @index_vars[index].active?
        end.to_set
      end

      # Return relevant data on the results of the ILP
      def result
        result = Results.new self
        result.enumerated_indexes = indexes
        result.indexes = selected_indexes
        result.total_size = selected_indexes.map(&:size).inject(0, &:+)
        result.total_cost = @objective_value

        result
      end

      # Get the size of all indexes in the workload
      def total_size
        @indexes.map do |index|
          @index_vars[index] * (index.size * 1.0)
        end.reduce(&:+)
      end

      # Get the cost of all queries in the workload
      def total_cost
        cost = @queries.map do |query|
          @indexes.map do |index|
            total_query_cost @data[:costs][query][index],
              @query_vars[index][query]
          end.compact
        end.flatten.inject(&:+)

        cost = add_update_costs cost, data
        cost
      end

      # The total number of indexes
      def total_indexes
        total = Gurobi::LinExpr.new
        @index_vars.values.each { |var| total += var * 1.0 }

        total
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
        define_objective

        log_model 'Model', '.lp'
      end

      private

      # Set the value of the objective function (workload cost)
      def define_objective(var_name = 'objective')
        obj = case @objective_type
              when Objective::COST
                total_cost
              when Objective::SPACE
                total_size
              when Objective::INDEXES
                total_indexes
              end

        # Add the objective function as a variable
        @obj_var = @model.addVar 0, Gurobi::INFINITY,
                                 1.0, Gurobi::CONTINUOUS, var_name
        @model.update
        @model.addConstr(@obj_var * 1.0 == obj)

        @logger.debug { "Objective function is #{obj.inspect}" }
        @objective = obj
        @model.setObjective @obj_var * 1.0, Gurobi::MINIMIZE
        @model.update
      end

      # Initialize query and index variables
      def add_variables
        @index_vars = {}
        @query_vars = {}
        @indexes.each do |index|
          @index_vars[index] = @model.addVar 0, 1, 0, Gurobi::BINARY, index.key
          @query_vars[index] = {}
          @queries.each_with_index do |query, q|
            query_var = "q#{q}_#{index.key}"
            @query_vars[index][query] = @model.addVar 0, 1, 0, Gurobi::BINARY,
                                                      query_var
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

        @logger.debug do
          "Added #{@model.getConstrs.count} constraints to model"
        end
      end

      # Deal with updates which do not require support queries
      def add_update_costs(min_cost, data)
        @updates.each do |update|
          @indexes.each do |index|
            next if !update.modifies_index?(index)

            min_cost += @index_vars[index] * data[:update_costs][update][index]
          end
        end

        min_cost
      end

      # Get the total cost of the query for the objective function
      def total_query_cost(cost, query_var)
        return if cost.nil?
        query_cost = cost.last * 1.0

        query_var * query_cost
      end
    end

    # Thrown when no solution can be found to the ILP
    class NoSolutionException < StandardError
    end
  end
end

# Only do this if Gurobi is available
if Object.const_defined? 'Gurobi'
  Gurobi::LinExpr.class_eval do
    def inspect
      # Get all the coefficients of variable terms
      expr = 0.upto(size - 1).map do |i|
        coeff = getCoeff i
        var = getVar(i).get_string Gurobi::StringAttr::VAR_NAME
        "#{coeff} * #{var}"
      end.join(' + ')

      # Possibly add a constant
      const = getConstant
      expr += " + #{const}" if const != 0

      expr
    end

    # Reduce this expression to one only involving active variables
    def active
      active_expr = Gurobi::LinExpr.new
      0.upto(size - 1).each do |i|
        coeff = getCoeff i
        var = getVar i
        active_expr += var * coeff if var.active?
      end

      active_expr
    end
  end

  Gurobi::Var.class_eval do
    # Check if this variable is active
    def active?
      # Even though we specifed the variables as binary, rounding
      # error means the value won't be exactly one
      # (the check exists to catch weird values if they arise)
      val = get_double Gurobi::DoubleAttr::X
      fail if val > 0.01 && val < 0.99
      val > 0.99
    end
  end
end
