module NoSE::Cost
  # A cost model which estimates the number of requests to the backend
  class RequestCountCost < Cost
    include Subtype

    # Rough cost estimate as the number of requests made
    # @return [Numeric]
    def self.index_lookup_cost(step)
      # We always start with a single lookup, then the number
      # of lookups is determined by the cardinality at the preceding step
      step.parent.is_a?(NoSE::Plans::RootPlanStep) ? \
        1 : step.parent.state.cardinality
    end

    # Cost estimate as number of entities deleted
    def self.delete_cost(step)
      step.state.cardinality
    end

    # Cost estimate as number of entities inserted
    def self.insert_cost(step)
      step.state.cardinality
    end
  end
end
