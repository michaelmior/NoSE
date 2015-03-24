
module NoSE::CLI
  # Add a command to run the advisor and benchmarks for a given workload
  class NoSECLI < Thor
    desc 'workload-bench NAME', 'run the workload NAME and benchmarks'
    def workload_bench(name)
      # Open a tempfile which will be used for advisor output
      filename = Tempfile.new('workload').path

      # Set some default options for various commands
      options = options.to_h
      options[:output] = filename
      options[:format] = 'json'
      options[:skip_existing] = true

      o = Thor::CoreExt::HashWithIndifferentAccess.new(options.select do |key|
        self.class.commands['workload'].options.keys.map(&:to_sym).include? \
          key.to_sym
      end)
      invoke self.class, :workload, [name], o

      o = Thor::CoreExt::HashWithIndifferentAccess.new(options.select do |key|
        self.class.commands['create'].options.keys.map(&:to_sym).include? \
          key.to_sym
      end)
      invoke self.class, :create, [filename], o

      o = Thor::CoreExt::HashWithIndifferentAccess.new(options.select do |key|
        self.class.commands['load'].options.keys.map(&:to_sym).include? \
          key.to_sym
      end)
      invoke self.class, :load, [filename], o

      o = Thor::CoreExt::HashWithIndifferentAccess.new(options.select do |key|
        self.class.commands['benchmark'].options.keys.map(&:to_sym).include? \
          key.to_sym
      end)
      invoke self.class, :benchmark, [filename], o
    end
    commands['workload_bench'].options.merge! commands['create'].options
    commands['workload_bench'].options.merge! commands['benchmark'].options
    commands['workload_bench'].options.merge! commands['load'].options
    commands['workload_bench'].options.merge! commands['workload'].options
  end
end
