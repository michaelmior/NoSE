# frozen_string_literal: true

module NoSE
  module Cost
    # A cost model which estimates the number of entities transferred
    class EntityCountCost < Cost
      include Subtype

      # Rough cost estimate as the number of entities retrieved at each step
      # @return [Numeric]
      def index_lookup_cost(step)
        # Simply count the number of entities at each step
        step.state.cardinality
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
