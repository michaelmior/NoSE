module NoSE::Cost
  class CassandraCost < Cost
    include Subtype

    # Rough cost estimate as the size of data returned
    # @return [Numeric]
    def self.index_lookup_cost(step)
      if step.state.answered?
        # If we have an answer to the query, we only need
        # to fetch the data fields which are selected
        fields = step.index.hash_fields + step.index.order_fields +
                 step.index.extra & step.state.query.select
      else
        fields = step.index.all_fields
      end

      step.state.cardinality * fields.map(&:size).inject(0, :+)
    end
  end
end
