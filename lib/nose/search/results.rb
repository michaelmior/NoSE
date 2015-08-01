module NoSE
  module Search
    # A container for results from a schema search
    class Results
      attr_accessor :enumerated_indexes, :indexes, :total_size, :total_cost,
                    :workload, :update_plans, :plans, :cost_model
      attr_accessor :revision, :time, :command

      def initialize(problem = nil)
        @problem = problem
        return if problem.nil?

        # Find the indexes the ILP says the query should use
        @query_indexes = Hash.new { |h, k| h[k] = Set.new }
        @problem.query_vars.each do |index, query_vars|
          query_vars.each do |query, var|
            next unless var.active?
            @query_indexes[query].add index
          end
        end
      end

      # Validate that the results of the search are consistent
      def validate
        validate_indexes
        validate_query_indexes @plans
        validate_update_indexes

        planned_queries = plans.map(&:query).to_set
        fail InvalidResultsException unless \
          (@workload.queries.to_set - planned_queries).empty?
        validate_query_plans @plans

        validate_update_plans
        validate_objective

        freeze
      end

      # Set the query plans which should be used based on the entire tree
      def plans_from_trees(trees)
        @plans = trees.map do |tree|
          # Exclude support queries since they will be in update plans
          query = tree.query
          next if query.is_a?(SupportQuery)

          select_plan tree
        end.compact
      end

      # Select the single query plan from a tree of plans
      def select_plan(tree)
        query = tree.query
        plan = tree.find do |plan|
          plan.indexes.to_set ==  @query_indexes[query]
        end
        plan.instance_variable_set :@weight,
                                   @workload.statement_weights[plan.query]

        fail InvalidResultsException if plan.nil?
        plan
      end

      private

      # Check that the indexes selected were actually enumerated
      def validate_indexes
        fail InvalidResultsException unless \
          (@indexes - @enumerated_indexes).empty?
      end

      # Ensure we only have necessary update plans which use available indexes
      def validate_update_indexes
        @update_plans.each do |plan|
          validate_query_indexes plan.query_plans
          valid_plan = @indexes.include?(plan.index)
          fail InvalidResultsException unless valid_plan
        end
      end

      # Check that the objective function has the expected value
      def validate_objective
        if @problem.objective_type == Objective::COST
          query_cost = @plans.reduce 0 do |sum, plan|
            sum + @workload.statement_weights[plan.query] * plan.cost
          end
          update_cost = @update_plans.reduce 0 do |sum, plan|
            sum + @workload.statement_weights[plan.statement] * plan.cost
          end
          cost = query_cost + update_cost

          fail InvalidResultsException unless (cost - @total_cost).abs < 0.001
        elsif @problem.objective_type == Objective::SPACE
          size = @indexes.map(&:size).inject(0, &:+)
          fail InvalidResultsException unless (size - @total_size).abs < 0.001
        end
      end

      # Ensure that all the query plans use valid indexes
      def validate_query_indexes(plans)
        plans.each do |plan|
          plan.each do |step|
            valid_plan = !step.is_a?(Plans::IndexLookupPlanStep) ||
                         @indexes.include?(step.index)
            fail InvalidResultsException unless valid_plan
          end
        end
      end

      # Validate the query plans from the original workload
      def validate_query_plans(plans)
        # Check that these indexes are actually used by the query
        plans.each do |plan|
          fail InvalidResultsException unless \
            plan.indexes.to_set == @query_indexes[plan.query]
        end
      end

      # Validate the support query plans for each update
      def validate_update_plans
        @update_plans.each do |plan|
          plan.instance_variable_set :@weight,
                                     workload.statement_weights[plan.statement]

          validate_query_plans plan.query_plans
        end
      end
    end

    # Thrown when a search produces invalid results
    class InvalidResultsException < StandardError
    end
  end
end
