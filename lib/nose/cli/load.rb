module NoSE
  module CLI
    # Add a command to load index data into a backend from a configured loader
    class NoSECLI < Thor
      desc 'load PLAN_FILE', 'create indexes from the given PLAN_FILE'
      option :progress, type: :boolean, default: true, aliases: '-p'
      option :limit, type: :numeric, default: nil, aliases: '-l'
      def load(plan_file)
        result = load_results(plan_file)
        backend = get_backend(options, result)

        # Create a new instance of the loader class
        loader_class = get_class 'loader', options
        loader = loader_class.new result.workload, backend

        # Remove the name from the options and execute the loader
        loader.load result.indexes, options[:loader], options[:progress]
      end
    end
  end
end
