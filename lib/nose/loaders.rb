module NoSE
  # Superclass for all data loaders
  class Loader
    def initialize(workload, backend)
      @workload = workload
      @backend = backend
    end
  end
end
