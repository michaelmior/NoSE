# frozen_string_literal: true

module NoSE
  module Cost
    # A cost model which estimates the number of requests to the backend
    class RequestCountCost < Cost
      include Subtype

      # Rough cost estimate as the number of requests made
      # @return [Numeric]
      def index_lookup_cost(step)
        # We always start with a single lookup, then the number
        # of lookups is determined by the cardinality at the preceding step
        if step.parent.is_a?(Plans::RootPlanStep)
          1
        else
          step.state.cardinality
        end
      end

      # Cost estimate as number of entities deleted
      def delete_cost(step)
        step.state.cardinality
      end

      # Cost estimate as number of entities inserted
      def insert_cost(step)
        step.state.cardinality
      end
    end
  end
end
