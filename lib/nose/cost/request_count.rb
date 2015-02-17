module NoSE::Cost
  class RequestCountCost < Cost
    include Subtype

    # Rough cost estimate as the number of requests made
    # @return [Numeric]
    def self.index_lookup_cost(step)
      # We always start with a single lookup, then the number
      # of lookups is determined by the cardinality at the preceding step
      step.parent_index.nil? ? 1 : step.parent.state.cardinality
    end
  end
end
