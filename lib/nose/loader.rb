module NoSE::Loader
  # Superclass for all data loaders
  class LoaderBase
    def initialize(workload, backend)
      @workload = workload
      @backend = backend
    end

    # @abstract Subclasses should load data for the given list of indexes
    # :nocov:
    def load(indexes, config, show_progress = false)
      raise NotImplementedError
    end
    # :nocov:

    # @abstract Subclasses should generate a model from the external source
    # :nocov:
    def model(config)
      raise NotImplementedError
    end
    # :nocov:
  end
end
