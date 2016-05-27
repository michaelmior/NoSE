require 'logging'

begin
  require 'mipper'
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
        # @model.write '/tmp/search.lp'
        @model.optimize

        # Ensure we found a valid solution
        @status = model.status
        if @status != :optimized
          fail NoSolutionException.new @status
        elsif @objective_type != Objective::INDEXES
          @objective_value = @model.objective_value

          # Pin the objective value and optimize again to minimize index usage
          @obj_var.lower_bound = @objective_value
          @obj_var.upper_bound = @objective_value
          @objective_type = Objective::INDEXES
          define_objective 'objective_indexes'

          @status = nil
          solve
          return
        elsif @objective_value.nil?
          @objective_value = @model.objective_value
        end

        @logger.debug do
          "Final objective value is #{@objective.inspect}" \
            " = #{@objective_value}"
        end
      end

      # Return the selected indices
      def selected_indexes
        return if @status.nil?
        return @selected_indexes if @selected_indexes

        @selected_indexes = @indexes.select do |index|
          @index_vars[index].value
        end.to_set
      end

      # Return relevant data on the results of the ILP
      def result
        result = Results.new self
        result.enumerated_indexes = indexes
        result.indexes = selected_indexes
        result.total_size = selected_indexes.sum_by(&:size)
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
        cost = @queries.reduce(MIPPeR::LinExpr.new) do |expr, query|
          expr.add(@indexes.reduce(MIPPeR::LinExpr.new) do |subexpr, index|
            subexpr.add total_query_cost(@data[:costs][query][index],
                                         @query_vars[index][query])
          end)
        end

        cost = add_update_costs cost
        cost
      end

      # The total number of indexes
      def total_indexes
        total = MIPPeR::LinExpr.new
        @index_vars.each_value { |var| total += var * 1.0 }

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
        @model = MIPPeR::CbcModel.new
        add_variables
        @model.update
        add_constraints
        define_objective
        @model.update

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
        @obj_var = MIPPeR::Variable.new 0, Float::INFINITY, 1.0,
                                        :continuous, var_name
        @model << @obj_var
        @model.update

        @model << MIPPeR::Constraint.new(obj + @obj_var * -1.0, :==, 0.0)

        @logger.debug { "Objective function is #{obj.inspect}" }

        @objective = obj
        @model.sense = :min
      end

      # Initialize query and index variables
      def add_variables
        @index_vars = {}
        @query_vars = {}
        @indexes.each do |index|
          @index_vars[index] = MIPPeR::Variable.new 0, 1, 0, :binary, index.key
          @model << @index_vars[index]

          @query_vars[index] = {}
          @queries.each_with_index do |query, q|
            query_var = "q#{q}_#{index.key}"
            var = MIPPeR::Variable.new 0, 1, 0, :binary, query_var
            @model << var
            @query_vars[index][query] = var
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
          "Added #{@model.constraints.count} constraints to model"
        end
      end

      # Deal with updates which do not require support queries
      def add_update_costs(min_cost)
        @updates.each do |update|
          @indexes.each do |index|
            next unless update.modifies_index?(index)

            min_cost.add @index_vars[index] *
              @data[:update_costs][update][index]
          end
        end

        min_cost
      end

      # Get the total cost of the query for the objective function
      def total_query_cost(cost, query_var)
        return MIPPeR::LinExpr.new if cost.nil?
        query_cost = cost.last * 1.0

        query_var * query_cost
      end
    end

    # Thrown when no solution can be found to the ILP
    class NoSolutionException < StandardError
    end
  end
end
