module NoSE::Loader
  # Superclass for all data loaders
  class LoaderBase
    def initialize(workload, backend)
      @workload = workload
      @backend = backend
    end

    # Implemented by subclasses to load data for the given list of indexes
    def load(indexes, config, show_progress = false)
      raise NotImplementedError
    end

    # Implemented by subclasses to generate a model from the external source
    def model(config)
      raise NotImplementedError
    end
  end
end
