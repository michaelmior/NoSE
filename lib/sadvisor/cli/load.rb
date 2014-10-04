module Sadvisor
  class SadvisorCLI < Thor
    desc 'load PLAN_FILE DIRECTORY', 'create indexes from the given PLAN_FILE'
    option :progress, type: :boolean, default: true
    def load(plan_file, directory)
      result = load_results(plan_file)
      config = load_config
      backend = get_backend(config, result)

      loader = CSVLoader.new result.workload, backend
      loader.load result.indexes, directory, options[:progress]
    end
  end
end
