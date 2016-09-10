# frozen_string_literal: true

module NoSE
  # Loaders which insert data into indexes from external sources
  module Loader
    # Superclass for all data loaders
    class LoaderBase
      def initialize(workload = nil, backend = nil)
        @workload = workload
        @backend = backend
      end

      # :nocov:
      # @abstract Subclasses should produce a workload
      # @return [void]
      def workload(_config)
        fail NotImplementedError
      end
      # :nocov:

      # :nocov:
      # @abstract Subclasses should load data for the given list of indexes
      # @return [void]
      def load(_indexes, _config, _show_progress = false, _limit = nil,
               _skip_existing = true)
        fail NotImplementedError
      end
      # :nocov:

      # @abstract Subclasses should generate a model from the external source
      # :nocov:
      def model(_config)
        fail NotImplementedError
      end
      # :nocov:
    end
  end
end
