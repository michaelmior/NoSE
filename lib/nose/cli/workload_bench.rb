
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

      o = Thor::CoreExt::HashWithIndifferentAccess.new(opts.select do |key|
        self.class.commands['workload'].options.keys.map(&:to_sym).include? \
          key.to_sym
      end)
      $stderr.puts "Running advisor #{o}..."
      invoke self.class, :workload, [name], o

      invoke self.class, :reformat, [filename], {}

      o = Thor::CoreExt::HashWithIndifferentAccess.new(opts.select do |key|
        self.class.commands['create'].options.keys.map(&:to_sym).include? \
          key.to_sym
      end)
      $stderr.puts "Creating indexes #{o}..."
      invoke self.class, :create, [filename], o

      o = Thor::CoreExt::HashWithIndifferentAccess.new(opts.select do |key|
        self.class.commands['load'].options.keys.map(&:to_sym).include? \
          key.to_sym
      end)
      $stderr.puts "Loading data #{o}..."
      invoke self.class, :load, [filename], o

      o = Thor::CoreExt::HashWithIndifferentAccess.new(opts.select do |key|
        self.class.commands['benchmark'].options.keys.map(&:to_sym).include? \
          key.to_sym
      end)
      $stderr.puts "Running benchmark #{o}..."
      invoke self.class, :benchmark, [filename], o
    end
    commands['workload_bench'].options.merge! commands['create'].options
    commands['workload_bench'].options.merge! commands['benchmark'].options
    commands['workload_bench'].options.merge! commands['load'].options
    commands['workload_bench'].options.merge! commands['workload'].options
  end
end
