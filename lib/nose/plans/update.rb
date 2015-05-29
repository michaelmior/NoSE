module NoSE
  module Plans
    # A superclass for steps which modify indexes
    class UpdatePlanStep < PlanStep
      attr_reader :index, :state

      def initialize(index, type, state = nil)
        super()
        @index = index
        @type = type

        return if state.nil?
        @state = state.dup
        @state.freeze
      end

      # :nocov:
      def to_color
        "#{super} #{@index.to_color}"
      end
      # :nocov:

      # Two insert steps are equal if they use the same index
      def ==(other)
        other.instance_of?(self.class) && @index == other.index && \
          @type == other.instance_variable_get(:@type)
      end
      alias_method :eql?, :==

      def hash
        [@index, @type].hash
      end
    end

    # A step which inserts data into a given index
    class InsertPlanStep < UpdatePlanStep
      def initialize(index, state = nil)
        super index, :insert, state
      end
    end

    # A step which deletes data into a given index
    class DeletePlanStep < UpdatePlanStep
      def initialize(index, state = nil)
        super index, :delete, state
      end
    end
  end
end
