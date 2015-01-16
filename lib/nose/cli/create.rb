module NoSE::CLI
  # Add a command for creating the index data structures in a backend
  class NoSECLI < Thor
    desc 'create PLAN_FILE', 'create indexes from the given PLAN_FILE'
    option :dry_run, type: :boolean, default: false
    def create(plan_file)
      result = load_results(plan_file)
      config = load_config
      backend = get_backend(config, result)

      # Produce the DDL and execute unless the dry run option was given
      backend.indexes_ddl(!options[:dry_run]).each do |ddl|
        puts ddl
      end
    end
  end
end
