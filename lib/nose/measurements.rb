require 'descriptive_statistics/safe'

module NoSE
  # Storage and presentation of value from performance measusrements
  module Measurements
    # A measurement of a single statement execution time
    class Measurement
      attr_reader :plan, :name, :weight

      # Allow the values array to store numbers and compute stats
      extend Forwardable
      def_delegators :@values, :each, :<<

      include Enumerable
      include DescriptiveStatistics

      def initialize(plan, name = nil, weight: 1.0)
        @plan = plan
        @name = name || (plan && plan.name)
        @weight = weight
        @values = []
      end

      # The mean weighted by this measurement weight
      def weighted_mean
        @weight * mean
      end
    end
  end
end
