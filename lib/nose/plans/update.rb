# frozen_string_literal: true

module NoSE
  module Plans
    # A superclass for steps which modify indexes
    class UpdatePlanStep < PlanStep
      attr_reader :index
      attr_accessor :state

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
        "#{super} #{@index.to_color} * #{@state.cardinality}"
      end
      # :nocov:

      # Two insert steps are equal if they use the same index
      def ==(other)
        other.instance_of?(self.class) && @index == other.index && \
          @type == other.instance_variable_get(:@type)
      end
      alias eql? ==

      def hash
        [@index, @type].hash
      end
    end

    # A step which inserts data into a given index
    class InsertPlanStep < UpdatePlanStep
      attr_reader :fields

      def initialize(index, state = nil, fields = Set.new)
        super index, :insert, state
        @fields = if fields.empty?
                    index.all_fields
                  else
                    fields.to_set & index.all_fields
                  end
        @fields += index.hash_fields + index.order_fields.to_set
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
