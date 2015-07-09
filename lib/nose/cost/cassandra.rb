module NoSE
  module Cost
    # A cost model which estimates the number of requests to the backend
    class CassandraCost < Cost
      include Subtype

      # Rough cost estimate as the number of requests made
      # @return [Numeric]
      def index_lookup_cost(step)
        # We always start with a single lookup, then the number
        # of lookups is determined by the cardinality at the preceding step
        if step.parent.is_a?(Plans::RootPlanStep)
          cardinality = 1
        else
          cardinality = step.parent.state.cardinality
        end
        parts = step.index.hash_count
        rows = step.index.per_hash_count

        # Scale by the percentage of data actually accessed
        parts *= cardinality * 1.0 / (rows + parts)
        @options[:partition_overhead] + parts * @options[:partition_cost] + \
          @options[:row_overhead] + rows * @options[:row_cost]
      end

      # Cost estimate as number of entities deleted
      def delete_cost(step)
        step.state.cardinality * @options[:delete_cost]
      end

      # Cost estimate as number of entities inserted
      def insert_cost(step)
        step.state.cardinality * @options[:insert_cost]
      end
    end
  end
end
