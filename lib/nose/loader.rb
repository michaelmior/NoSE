# Loaders which insert data into indexes from external sources
module NoSE::Loader
  # Superclass for all data loaders
  class LoaderBase
    def initialize(workload, backend)
      @workload = workload
      @backend = backend
    end

    # @abstract Subclasses should load data for the given list of indexes
    # :nocov:
    def load(_indexes, _config, _show_progress = false)
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
