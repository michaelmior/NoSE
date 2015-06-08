module NoSE
  module Search
    # A container for results from a schema search
    class Results
      attr_accessor :enumerated_indexes, :indexes, :total_size, :total_cost,
                    :workload, :update_plans, :plans, :cost_model

      # Validate that the results of the search are consistent
      def validate
        validate_indexes
        validate_query_plans @plans
        validate_update_plans
        validate_cost

        freeze
      end

      private

      # Check that the indexes selected were actually enumerated
      def validate_indexes
        fail InvalidResultsException unless \
          (@indexes - @enumerated_indexes).empty?
      end

      # Ensure we only have necessary update plans which use available indexes
      def validate_update_plans
        @update_plans.each do |plan|
          validate_query_plans plan.query_plans
          valid_plan = @indexes.include?(plan.index)
          fail InvalidResultsException unless valid_plan
        end
      end

      # Check that the cost has the expected value
      def validate_cost
        query_cost = @plans.reduce 0 do |sum, plan|
          sum + @workload.statement_weights[plan.query] * plan.cost
        end
        update_cost = @update_plans.reduce 0 do |sum, plan|
          sum + @workload.statement_weights[plan.statement] * plan.cost
        end
        cost = query_cost + update_cost

        fail InvalidResultsException unless (cost - @total_cost).abs < 0.001
      end

      # Ensure that all the query plans use valid indexes
      def validate_query_plans(plans)
        plans.each do |plan|
          plan.each do |step|
            valid_plan = !step.is_a?(Plans::IndexLookupPlanStep) ||
                         @indexes.include?(step.index)
            fail InvalidResultsException unless valid_plan
          end
        end
      end
    end

    # Thrown when a search produces invalid results
    class InvalidResultsException < StandardError
    end
  end
end
