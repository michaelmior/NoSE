# frozen_string_literal: true

module NoSE
  module CLI
    # Add a command to run the advisor and benchmarks for a given workload
    class NoSECLI < Thor
      desc 'search-bench NAME', 'run the workload NAME and benchmarks'

      long_desc <<-LONGDESC
        `nose search-bench` is a convenient way to quickly test many different
        schemas. It will run `nose search`, `nose create`, `nose load`, and
        `nose benchmark` to produce final benchmark results. It also accepts
        all of the command-line options for each of those commands.
      LONGDESC

      def search_bench(name)
        # Open a tempfile which will be used for advisor output
        filename = Tempfile.new('workload').path

        # Set some default options for various commands
        opts = options.to_h
        opts[:output] = filename
        opts[:format] = 'json'
        opts[:skip_existing] = true

        o = filter_command_options opts, 'search'
        $stderr.puts "Running advisor #{o}..."
        invoke self.class, :search, [name], o

        invoke self.class, :reformat, [filename], {}

        o = filter_command_options opts, 'create'
        $stderr.puts "Creating indexes #{o}..."
        invoke self.class, :create, [filename], o

        o = filter_command_options opts, 'load'
        $stderr.puts "Loading data #{o}..."
        invoke self.class, :load, [filename], o

        o = filter_command_options opts, 'benchmark'
        $stderr.puts "Running benchmark #{o}..."
        invoke self.class, :benchmark, [filename], o
      end

      # Allow this command to accept the options for all commands it calls
      commands['search_bench'].options.merge! commands['create'].options
      commands['search_bench'].options.merge! commands['benchmark'].options
      commands['search_bench'].options.merge! commands['load'].options
      commands['search_bench'].options.merge! commands['search'].options
    end
  end
end
