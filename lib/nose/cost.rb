module NoSE::Cost
  # Cost model for a backend database
  class Cost
    include Supertype

    # The cost of filtering intermediate results
    def self.filter_cost(_step)
      # Assume this has no cost and the cost is captured in the fact that we
      # have to retrieve more data earlier. All this does is skip records.
      1
    end

    # The cost of limiting a result set
    def self.limit_cost(_step)
      # This is basically free since we just discard data
      0
    end

    # (see PlanStep#cost)
    def self.sort_cost(_step)
      # TODO: Find some estimate of sort cost
      #       This could be partially captured by the fact that sort + limit
      #       effectively removes the limit
      1
    end

    # The cost of performing a lookup via an index
    def self.index_lookup_cost(_step)
      fail NotImplementedError, 'Must be implemented in a subclass'
    end

    # This is here for debugging purposes because we need a cost
    def self.pruned_cost(_step)
      0
    end
  end
end

require_relative 'cost/entity_count'
require_relative 'cost/field_size'
require_relative 'cost/request_count'
