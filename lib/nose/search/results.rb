module NoSE
  module Search
    # A container for results from a schema search
    class Results
      attr_accessor :enumerated_indexes, :indexes, :total_size, :total_cost,
                    :workload, :update_plans, :plans, :cost_model

      # Validate that the results of the search are consistent
      def validate
        fail unless valid_indexes?
        fail unless valid_query_plans? @plans
        fail unless valid_update_plans?
        fail unless valid_cost?

        freeze
      end

      private

      # Check that the indexes selected were actually enumerated
      def valid_indexes?
        (@indexes - @enumerated_indexes).empty?
      end

      # Ensure we only have necessary update plans which use available indexes
      def valid_update_plans?
        @update_plans.each do |plan|
          @indexes.include?(plan.index) && valid_query_plans?(plan.query_plans)
        end
      end

      # Check that the cost has the expected value
      def valid_cost?
        query_cost = @plans.reduce 0 do |sum, plan|
          sum + @workload.statement_weights[plan.query] * plan.cost
        end
        update_cost = @update_plans.reduce 0 do |sum, plan|
          sum + @workload.statement_weights[plan.statement] * plan.cost
        end
        cost = query_cost + update_cost

        (cost - @total_cost).abs < 0.001
      end

      # Ensure that all the query plans use valid indexes
      def valid_query_plans?(plans)
        plans.all? do |plan|
          plan.all? do |step|
            !step.is_a?(Plans::IndexLookupPlanStep) ||
              @indexes.include?(step.index)
          end
        end
      end
    end
  end
end
