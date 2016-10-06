# frozen_string_literal: true

require 'ostruct'

module NoSE
  module CLI
    # Add a command to dump a workload and its corresponding schema
    class NoSECLI < Thor
      desc 'dump PLANS', 'output the plans in PLANS'

      long_desc <<-LONGDESC
        `nose dump` will output results in the same format as `nose search`,
        but for manually defined execution plans in the `plans` subdirectory.
      LONGDESC

      shared_option :format
      shared_option :mix

      def dump(plan_name)
        plans = Plans::ExecutionPlans.load plan_name
        plans.mix = options[:mix].to_sym \
          unless options[:mix] == 'default' && plans.mix != :default

        # Set the cost of each plan
        cost_model = get_class_from_config options, 'cost', :cost_model
        plans.calculate_cost cost_model

        results = OpenStruct.new
        results.workload = Workload.new plans.schema.model
        results.workload.mix = plans.mix
        results.model = results.workload.model
        results.indexes = plans.schema.indexes.values
        results.enumerated_indexes = []

        results.plans = []
        results.update_plans = []

        # Store all the query and update plans
        plans.groups.values.flatten(1).each do |plan|
          if plan.update_steps.empty?
            results.plans << plan
          else
            # XXX: Hack to build a valid update plan
            statement = OpenStruct.new group: plan.group
            update_plan = Plans::UpdatePlan.new statement, plan.index, nil,
                                                plan.update_steps, cost_model
            update_plan.instance_variable_set :@group, plan.group
            update_plan.instance_variable_set :@query_plans, plan.query_plans
            results.update_plans << update_plan
          end
        end

        results.cost_model = cost_model
        results.weights = Hash[plans.weights.map { |g, w| [g, w[plans.mix]] }]
        results.total_size = results.indexes.sum_by(&:size)
        results.total_cost = plans.groups.values.flatten(1).sum_by do |plan|
          next 0 if plan.weight.nil?

          plan.cost * plan.weight
        end

        # Output the results in the specified format
        send(('output_' + options[:format]).to_sym, results)
      end
    end
  end
end
