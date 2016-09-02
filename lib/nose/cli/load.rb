# frozen_string_literal: true

module NoSE
  module CLI
    # Add a command to load index data into a backend from a configured loader
    class NoSECLI < Thor
      desc 'load PLAN_FILE_OR_SCHEMA',
           'create indexes from the given PLAN_FILE_OR_SCHEMA'

      long_desc <<-LONGDESC
        `nose load` will load a schema either from generated plan file from
        `nose search` or a named schema in the `schemas` directory. It will
        then populate the backend indexes as defined by the schema using data
        from the configured loader. It assumes that the indexes have already
        been created by `nose create`.
      LONGDESC

      option :progress, type: :boolean, default: true, aliases: '-p',
                        desc: 'whether to display an indication of progress'
      option :limit, type: :numeric, default: nil, aliases: '-l',
                     desc: 'limit the number of entries loaded ' \
                           '(useful for testing)'
      option :skip_nonempty, type: :boolean, default: true, aliases: '-s',
                             desc: 'ignore indexes which are not empty'

      def load(*plan_files)
        plan_files.each { |plan_file| load_plan plan_file, options }
      end

      private

      # Load data from a single plan file
      # @return [void]
      def load_plan(plan_file, options)
        result, backend = load_plans plan_file, options

        # Create a new instance of the loader class and execute
        loader = get_class('loader', options).new result.workload, backend
        loader.load result.indexes, options[:loader], options[:progress],
                    options[:limit], options[:skip_nonempty]
      end
    end
  end
end
