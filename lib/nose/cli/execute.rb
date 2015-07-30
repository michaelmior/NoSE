module NoSE
  module CLI
    # Run performance tests on plans for a particular schema
    class NoSECLI < Thor
      desc 'execute PLANS', 'test performance of the named PLANS'
      option :num_iterations, type: :numeric, default: 100
      option :group, type: :string, default: nil, aliases: '-g'
      option :plan, type: :string, default: nil, aliases: '-p'
      def execute(plans_name)
        # Load the execution plans
        plans = ExecutionPlans.load plans_name

        # Construct an instance of the backend
        result = OpenStruct.new
        result.workload = plans.schema.workload
        result.indexes = plans.schema.indexes.values
        backend = get_backend(options, result)

        # Get sample index values to use in queries
        index_values = index_values plans.schema.indexes.values, backend,
                                    options[:num_iterations]

        plans.groups.each do |group, group_plans|
          next if options[:group] && group != options[:group]
          puts "Group #{group}"

          group_plans.each do |plan|
            next if options[:plan] && plan.name != options[:plan]

            update = !plan.steps.last.is_a?(Plans::IndexLookupPlanStep)
            if update
              # XXX Skip updates for now
              next
            else
              prepared = backend.prepare_query nil, plan.select, plan.params,
                                               [plan.steps]
            end

            # Construct a list of values to be substituted in the plan
            condition_list = 1.upto(options[:num_iterations]).map do |i|
              Hash[plan.params.map do |field_id, condition|
                value = plans.schema.indexes.values.each do |index|
                  values = index_values[index]
                  value = values[i % values.length][condition.field.id]
                  break value unless value.nil?
                end

                [
                  field_id,
                  Condition.new(condition.field, condition.operator, value)
                ]
              end]
            end

            # Execute each plan and measure the time
            start_time = Time.now
            condition_list.each { |conditions| prepared.execute conditions }
            elapsed = Time.now - start_time

            # Report the time taken
            puts "  #{plan.name} executed in " \
              "#{elapsed / options[:num_iterations]} average"
          end
        end
      end
    end
  end
end
