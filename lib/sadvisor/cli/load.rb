module Sadvisor
  class SadvisorCLI < Thor
    desc 'load PLAN_FILE', 'create indexes from the given PLAN_FILE'
    option :progress, type: :boolean, default: true
    def load(plan_file)
      result = load_results(plan_file)
      config = load_config
      backend = get_backend(config, result)

      # Create a new instance of the loader class
      require_relative "../loaders/#{config[:loader][:name]}"
      loaders = ObjectSpace.each_object(Sadvisor::Loader.singleton_class)
      loader = loaders.find do |klass|
        path = klass.instance_method(:load).source_location
        next if path.nil?
        !/\b#{config[:loader][:name]}\.rb/.match(path.first).nil?
      end
      loader = loader.new result.workload, backend

      # Remove the name from the config and execute the loader
      config.delete :name
      loader.load result.indexes, config[:loader], options[:progress]
    end
  end
end
