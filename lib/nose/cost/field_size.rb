# frozen_string_literal: true

module NoSE
  module Cost
    # A cost model which estimates the total size of data transferred
    class FieldSizeCost < Cost
      include Subtype

      # Rough cost estimate as the size of data returned
      # @return [Numeric]
      def index_lookup_cost(step)
        # If we have an answer to the query, we only need
        # to fetch the data fields which are selected
        fields = step.index.all_fields
        fields &= step.state.query.select if step.state.answered?

        step.state.cardinality * fields.sum_by(&:size)
      end

      # Cost estimate as the size of an index entry
      def delete_cost(step)
        step.index.entry_size
      end

      # Cost estimate as the size of an index entry
      def insert_cost(step)
        step.index.entry_size
      end
    end
  end
end
