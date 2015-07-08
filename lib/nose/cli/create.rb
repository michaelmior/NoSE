module NoSE
  module CLI
    # Add a command for creating the index data structures in a backend
    class NoSECLI < Thor
      desc 'create PLAN_FILE', 'create indexes from the given PLAN_FILE'
      option :dry_run, type: :boolean, default: false
      option :skip_existing, type: :boolean, default: false, aliases: '-s'
      option :drop_existing, type: :boolean, default: false, aliases: '-d'
      def create(plan_file)
        result = load_results(plan_file)
        backend = get_backend(options, result)

        # Produce the DDL and execute unless the dry run option was given
        backend.indexes_ddl(!options[:dry_run], options[:skip_existing],
                            options[:drop_existing]) \
          .each do |ddl|
            puts ddl
          end
      end
    end
  end
end
