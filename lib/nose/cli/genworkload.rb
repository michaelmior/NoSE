module NoSE
    module CLI
    # Add a command to generate a workload file from a given loader
    class NoSECLI < Thor
      desc 'genworkload NAME',
           'generate a workload called NAME from the configured loader'
      def genworkload(name)
        loader_class = get_class 'loader', options
        workload = loader_class.new.workload options[:loader]
        workload.output_rb "./workloads/#{name}.rb"
      end
    end
  end
end
