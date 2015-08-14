module NoSE
    module CLI
    # Add a command to dump a workload and its corresponding schema
    class NoSECLI < Thor
      desc 'dump PLANS', 'output the plans in PLANS'
      option :mix, type: :string, default: 'default'
      option :format, type: :string, default: 'txt',
                      enum: %w(txt json yml), aliases: '-f'
      def dump(plan_name)
        plans = Plans::ExecutionPlans.load plan_name
        plans.mix = options[:mix].to_sym \
          unless options[:mix] == 'default' && plans.mix != :default

        # Set the cost of each plan
        cost_model = get_class('cost', options[:cost_model][:name]).new(**options[:cost_model])
        plans.calculate_cost cost_model

        results = OpenStruct.new
        results.workload = Workload.new plans.schema.model
        results.indexes = plans.schema.indexes.values
        results.enumerated_indexes = []
        results.plans = plans.groups.values.flatten(1)
        results.update_plans = []
        results.cost_model = cost_model
        results.weights = plans.weights
        results.total_size = results.indexes.map(&:size).inject(0, &:+)
        results.total_cost = plans.groups.values.flatten(1).map do |plan|
          plan.cost * plan.weight
        end.inject(0, &:+)

        cost_model = get_class('cost', options[:cost_model][:name])
        results.cost_model = cost_model.new(**options[:cost_model])

        # Output the results in the specified format
        send(('output_' + options[:format]).to_sym, results)
      end
    end
  end
end

