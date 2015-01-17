module NoSE::CLI
  # Add a command to generate a workload file from a given loader
  class NoSECLI < Thor
    desc 'genworkload NAME',
         'generate a workload called NAME from the configured loader'
    def genworkload(name)
      config = load_config
      loader_class = get_class 'loader', config
      workload = loader_class.new.workload config[:loader]
      workload.output_rb "./workloads/#{name}.rb"
    end
  end
end
