require 'descriptive_statistics/safe'

module NoSE
  # Storage and presentation of value from performance measusrements
  module Measurements
    # A measurement of a single statement execution time
    class Measurement
      attr_reader :plan

      # Allow the values array to store numbers and compute stats
      extend Forwardable
      def_delegators :@values, :each, :<<

      include Enumerable
      include DescriptiveStatistics

      def initialize(plan)
        @plan = plan
        @values = []
      end
    end
  end
end
