
module NoSE::CLI
  # Add a command to run the advisor and benchmarks for a given workload
  class NoSECLI < Thor
    desc 'workload-bench NAME', 'run the workload NAME and benchmarks'
    def workload_bench(name)
      # Open a tempfile which will be used for advisor output
      filename = Tempfile.new('workload').path

      # Set some default options for various commands
      opts = options.to_h
      opts[:output] = filename
      opts[:format] = 'json'
      opts[:skip_existing] = true

      o = filter_command_options opts, 'workload'
      $stderr.puts "Running advisor #{o}..."
      invoke self.class, :workload, [name], o

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
    commands['workload_bench'].options.merge! commands['create'].options
    commands['workload_bench'].options.merge! commands['benchmark'].options
    commands['workload_bench'].options.merge! commands['load'].options
    commands['workload_bench'].options.merge! commands['workload'].options
  end
end
