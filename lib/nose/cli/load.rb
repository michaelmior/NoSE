module NoSE
  module CLI
    # Add a command to load index data into a backend from a configured loader
    class NoSECLI < Thor
      desc 'load PLAN_FILE_OR_SCHEMA', 'create indexes from the given PLAN_FILE_OR_SCHEMA'
      option :progress, type: :boolean, default: true, aliases: '-p'
      option :limit, type: :numeric, default: nil, aliases: '-l'
      option :skip_existing, type: :boolean, default: true, aliases: '-s'
      def load(plan_file)
        if File.exist? plan_file
          result = load_results(plan_file)
        else
          schema = Schema.load plan_file
          result = OpenStruct.new
          result.workload = schema.workload
          result.indexes = schema.indexes.values
        end
        backend = get_backend(options, result)

        # Create a new instance of the loader class
        loader_class = get_class 'loader', options
        loader = loader_class.new result.workload, backend

        # Remove the name from the options and execute the loader
        loader.load result.indexes, options[:loader], options[:progress],
                    options[:limit], options[:skip_existing]
      end
    end
  end
end
