module NoSE::CLI
  # Add a command to load index data into a backend from a configured loader
  class NoSECLI < Thor
    desc 'load PLAN_FILE', 'create indexes from the given PLAN_FILE'
    option :progress, type: :boolean, default: true
    def load(plan_file)
      result = load_results(plan_file)
      config = load_config
      backend = get_backend(config, result)

      # Create a new instance of the loader class
      loader_class = get_class 'loader', config
      loader = loader_class.new result.workload, backend

      # Remove the name from the config and execute the loader
      loader.load result.indexes, config[:loader], options[:progress]
    end
  end
end
