module NoSE
    module CLI
    # Add a command to dump a workload and its corresponding schema
    class NoSECLI < Thor
      desc 'dump PLANS', 'output the plans in PLANS'
      long_desc <<-LONGDESC
        `nose dump` will output results in the same format as `nose search`,
        but for manually defined execution plans in the `plans` subdirectory.

        Note: This does not currently include update plans
      LONGDESC
      option :mix, type: :string, default: 'default',
                   banner: 'the name of the workload mix for weighting queries'
      option :format, type: :string, default: 'txt',
                      enum: %w(txt json yml), aliases: '-f',
                      banner: 'the format of the produced plans'
      def dump(plan_name)
        plans = Plans::ExecutionPlans.load plan_name
        plans.mix = options[:mix].to_sym \
          unless options[:mix] == 'default' && plans.mix != :default

        # Set the cost of each plan
        cost_model = get_class_from_config options, 'cost', :cost_model
        plans.calculate_cost cost_model

        results = OpenStruct.new
        results.workload = Workload.new plans.schema.model
        results.indexes = plans.schema.indexes.values
        results.enumerated_indexes = []
        results.plans = plans.groups.values.flatten(1)
        # TODO: Add update plans
        results.cost_model = cost_model
        results.weights = plans.weights
        results.total_size = results.indexes.sum_by(&:size)
        results.total_cost = plans.groups.values.flatten(1).sum_by do |plan|
          plan.cost * plan.weight
        end

        # Output the results in the specified format
        send(('output_' + options[:format]).to_sym, results)
      end
    end
  end
end

