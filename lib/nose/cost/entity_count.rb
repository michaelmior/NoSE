module NoSE::Cost
  class EntityCountCost < Cost
    include Subtype

    # Rough cost estimate as the number of entities retrieved at each step
    # @return [Numeric]
    def self.index_lookup_cost(step)
      # Simply count the number of entities at each step
      step.state.cardinality
    end
  end
end
