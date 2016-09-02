# frozen_string_literal: true

module NoSE
  module Plans
    # A query plan performing a filter without an index
    class FilterPlanStep < PlanStep
      attr_reader :eq, :range

      def initialize(eq, range, state = nil)
        @eq = eq
        @range = range
        super()

        return if state.nil?
        @state = state.dup
        update_state
        @state.freeze
      end

      # Two filtering steps are equal if they filter on the same fields
      # @return [Boolean]
      def ==(other)
        other.instance_of?(self.class) && \
          @eq == other.eq && @range == other.range
      end

      def hash
        [@eq.map(&:id), @range.nil? ? nil : @range.id].hash
      end

      # :nocov:
      def to_color
        "#{super} #{@eq.to_color} #{@range.to_color} " +
          begin
            "#{@parent.state.cardinality} " \
              "-> #{state.cardinality}"
          rescue NoMethodError
            ''
          end
      end
      # :nocov:

      # Check if filtering can be done (we have all the necessary fields)
      def self.apply(parent, state)
        # Get fields and check for possible filtering
        filter_fields, eq_filter, range_filter = filter_fields parent, state
        return nil if filter_fields.empty?

        FilterPlanStep.new eq_filter, range_filter, state \
          if required_fields?(filter_fields, parent)
      end

      # Get the fields we can possibly filter on
      def self.filter_fields(parent, state)
        eq_filter = state.eq.select { |field| parent.fields.include? field }
        filter_fields = eq_filter.dup
        if state.range && parent.fields.include?(state.range)
          range_filter = state.range
          filter_fields << range_filter
        else
          range_filter = nil
        end

        [filter_fields, eq_filter, range_filter]
      end
      private_class_method :filter_fields

      # Check that we have all the fields we are filtering
      # @return [Boolean]
      def self.required_fields?(filter_fields, parent)
        filter_fields.map do |field|
          next true if parent.fields.member? field

          # We can also filter if we have a foreign key
          # XXX for now we assume this value is the same
          next unless field.is_a? IDField
          parent.fields.any? do |pfield|
            pfield.is_a?(ForeignKeyField) && pfield.entity == field.parent
          end
        end.all?
      end
      private_class_method :required_fields?

      private

      # Apply the filters and perform a uniform estimate on the cardinality
      # @return [void]
      def update_state
        @state.eq -= @eq
        @state.cardinality *= @eq.map { |field| 1.0 / field.cardinality } \
                                 .inject(1.0, &:*)
        return unless @range

        @state.range = nil
        @state.cardinality *= 0.1
      end
    end
  end
end
