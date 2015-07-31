module NoSE
    module CLI
    # Add a command to dump a workload and its corresponding schema
    class NoSECLI < Thor
      desc 'dump PLANS', 'output the plans in PLANS'
      option :format, type: :string, default: 'txt',
                      enum: %w(txt json yml), aliases: '-f'
      def dump(plan_name)
        plans = Plans::ExecutionPlans.load plan_name

        results = OpenStruct.new
        results.workload = plans.schema.workload
        results.indexes = plans.schema.indexes.values
        results.enumerated_indexes = []
        results.plans = plans.groups.values.flatten(1)
        results.update_plans = []

        cost_model = get_class('cost', options[:cost_model][:name])
        results.cost_model = cost_model.new(**options[:cost_model])

        # Output the results in the specified format
        send(('output_' + options[:format]).to_sym, results)
      end
    end
  end
end

