require 'descriptive_statistics/safe'

module NoSE
  module Measurements
    # A measurement of a single statement execution time
    class StatementMeasurement
      attr_reader :group, :name, :weight

      # Allow the values array to store numbers and compute stats
      extend Forwardable
      def_delegators :@values, :each, :<<

      include Enumerable
      include DescriptiveStatistics

      def initialize(group, name, weight)
        @group = group
        @name = name
        @weight = weight
        @values = []
      end
    end
  end
end
