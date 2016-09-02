# frozen_string_literal: true

module NoSE
  module Plans
    # Limit results from a previous lookup
    # This should only ever occur at the end of a plan
    class LimitPlanStep < PlanStep
      attr_reader :limit

      def initialize(limit, state = nil)
        super()
        @limit = limit

        return if state.nil?
        @state = state.dup
        @state.cardinality = @limit
      end

      # Two limit steps are equal if they have the same value for the limit
      def ==(other)
        other.instance_of?(self.class) && @limit == other.limit
      end
      alias eql? ==

      def hash
        @limit
      end

      # Check if we can apply a limit
      # @return [LimitPlanStep]
      def self.apply(_parent, state)
        # TODO: Apply if have IDs of the last entity set
        #       with no filter/sort needed

        return nil if state.query.limit.nil?
        return nil unless state.answered? check_limit: false

        LimitPlanStep.new state.query.limit, state
      end
    end
  end
end
