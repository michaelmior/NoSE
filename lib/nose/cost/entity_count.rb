module NoSE::Cost
  class EntityCountCost < Cost
    include Subtype

    # Rough cost estimate as the number of entities retrieved at each step
    # @return [Numeric]
    def self.index_lookup_cost(step)
      # Simply count the number of entities at each step
      step.state.cardinality
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
