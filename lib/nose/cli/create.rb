# frozen_string_literal: true

module NoSE
  module CLI
    # Add a command for creating the index data structures in a backend
    class NoSECLI < Thor
      desc 'create PLAN_FILE_OR_SCHEMA',
           'create indexes from the given PLAN_FILE_OR_SCHEMA'

      long_desc <<-LONGDESC
        `nose create` will load a schema either from generated plan file from
        `nose search` or a named schema in the `schemas` directory. It will
        then create all the indexes in the configured backend.
      LONGDESC

      option :dry_run, type: :boolean, default: false,
                       desc: 'print the DDL, but do not execute'
      option :skip_existing, type: :boolean, default: false, aliases: '-s',
                             desc: 'ignore indexes which already exist'
      option :drop_existing, type: :boolean, default: false,
                             desc: 'drop existing indexes before recreation'

      def create(*plan_files)
        plan_files.each do |plan_file|
          _, backend = load_plans plan_file, options

          # Produce the DDL and execute unless the dry run option was given
          backend.indexes_ddl(!options[:dry_run], options[:skip_existing],
                              options[:drop_existing]) \
                 .each { |ddl| puts ddl }
        end
      end
    end
  end
end
