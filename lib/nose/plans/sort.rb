module NoSE::Plans
  # A query plan step performing external sort
  class SortPlanStep < PlanStep
    attr_reader :sort_fields

    def initialize(sort_fields)
      super()
      @sort_fields = sort_fields
    end

    # :nocov:
    def to_color
      super + ' [' + @sort_fields.map(&:to_color).join(', ') + ']'
    end
    # :nocov:

    # Two sorting steps are equal if they sort on the same fields
    def ==(other)
      other.instance_of?(self.class) && @sort_fields == other.sort_fields
    end

    # (see PlanStep#cost)
    def cost
      # TODO: Find some estimate of sort cost
      #       This could be partially captured by the fact that sort + limit
      #       effectively removes the limit
      1
    end

    # Check if an external sort can used (if a sort is the last step)
    def self.apply(_parent, state)
      new_step = nil

      if state.fields.empty? && state.eq.empty? && state.range.nil? && \
          !state.order_by.empty?

        new_state = state.dup
        new_state.order_by = []
        new_step = SortPlanStep.new(state.order_by)
        new_step.state = new_state
        new_step.state.freeze
      end

      new_step
    end
  end
end
