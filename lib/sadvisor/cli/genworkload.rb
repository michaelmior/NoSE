module Sadvisor
  class SadvisorCLI < Thor
    desc 'genworkload NAME',
         'generate a workload called NAME from the configured loader'
    def genworkload(name)
      config = load_config
      loader_class = get_loader_class config
      workload = loader_class.new.workload config[:loader]
      workload.output_rb "./workloads/#{name}.rb"
    end
  end
end
