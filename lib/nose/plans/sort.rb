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
        Zlib.crc32 @sort_fields.map(&:id).to_s
      end

      # Check if an external sort can used (if a sort is the last step)
      # @return [SortPlanStep]
      def self.apply(_parent, state)
        return nil unless state.fields.empty? && state.eq.empty? && \
                          state.range.nil? && !state.order_by.empty?

        new_state = state.dup
        new_state.order_by = []
        new_step = SortPlanStep.new(state.order_by, new_state)
        new_step.state.freeze

        new_step
      end
    end
  end
end
