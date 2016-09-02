# frozen_string_literal: true

module NoSE
  module Plans
    # A query plan step performing external sort
    class SortPlanStep < PlanStep
      attr_reader :sort_fields

      def initialize(sort_fields, state = nil)
        super()

        @sort_fields = sort_fields
        @state = state
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
      alias eql? ==

      def hash
        @sort_fields.map(&:id).hash
      end

      # Check if an external sort can used (if a sort is the last step)
      # @return [SortPlanStep]
      def self.apply(parent, state)
        fetched_all_ids = state.fields.none? { |f| f.is_a? Fields::IDField }
        resolved_predicates = state.eq.empty? && state.range.nil?
        can_order = !(state.order_by.to_set & parent.fields).empty?
        return nil unless fetched_all_ids && resolved_predicates && can_order

        new_state = state.dup
        new_state.order_by = []
        new_step = SortPlanStep.new(state.order_by, new_state)
        new_step.state.freeze

        new_step
      end
    end
  end
end
