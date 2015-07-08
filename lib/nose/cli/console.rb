module NoSE
  module CLI
    # Start a pry console while preloading configured objects
    class NoSECLI < Thor
      desc 'console PLAN_FILE', 'open a pry console preconfigured with ' \
                                'variables from the given PLAN_FILE'
      def console(plan_file)
        # Load the results from the plan file and define each as a variable
        result = load_results plan_file
        result.each_pair do |name, value|
          TOPLEVEL_BINDING.local_variable_set name, value
        end

        # Also extract the model as a variable
        TOPLEVEL_BINDING.local_variable_set :model, result.workload.model

        # Load the options and backend as variables
        TOPLEVEL_BINDING.local_variable_set :options, options
        TOPLEVEL_BINDING.local_variable_set :backend,
                                            get_backend(options, result)

        TOPLEVEL_BINDING.pry
      end
    end
  end
end
