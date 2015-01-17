module NoSE::Loader
  # Superclass for all data loaders
  class LoaderBase
    def initialize(workload, backend)
      @workload = workload
      @backend = backend
    end
  end
end
