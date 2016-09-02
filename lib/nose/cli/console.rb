# frozen_string_literal: true

module NoSE
  module CLI
    # Start a pry console while preloading configured objects
    class NoSECLI < Thor
      desc 'console PLAN_FILE', 'open a pry console preconfigured with ' \
                                'variables from the given PLAN_FILE'

      long_desc <<-LONGDESC
        `nose console` gives a convenient way to perform manual exploration of
        generated plan data. It will load plans from the given file and then
        define a number of variables containing this data. This includes all
        instance variables in the `Search::Results` object as well as the
        `model` used to generate the results, the `options` loaded from the
        configuration file, and an instance of the configured `backend`.
      LONGDESC

      def console(plan_file)
        # Load the results from the plan file and define each as a variable
        result = load_results plan_file
        expose_result result

        # Also extract the model as a variable
        TOPLEVEL_BINDING.local_variable_set :model, result.workload.model

        # Load the options and backend as variables
        TOPLEVEL_BINDING.local_variable_set :options, options
        TOPLEVEL_BINDING.local_variable_set :backend,
                                            get_backend(options, result)

        TOPLEVEL_BINDING.pry
      end

      private

      # Expose the properties of the results object for use in the console
      # @return [void]
      def expose_result(result)
        exposed = result.instance_variables.map do |var|
          var[1..-1].to_sym
        end & result.methods

        exposed.each do |name|
          TOPLEVEL_BINDING.local_variable_set name, result.method(name).call
        end
      end
    end
  end
end
