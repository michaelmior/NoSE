module NoSE
  module CLI
    # Run performance tests on plans for a particular schema
    class NoSECLI < Thor
      desc 'execute PLANS', 'test performance of the named PLANS'
      option :num_iterations, type: :numeric, default: 100
      option :mix, type: :string, default: 'default'
      option :group, type: :string, default: nil, aliases: '-g'
      option :plan, type: :string, default: nil, aliases: '-p'
      option :fail_on_empty, type: :boolean, default: true
      def execute(plans_name)
        # Load the execution plans
        plans = Plans::ExecutionPlans.load plans_name

        # Construct an instance of the backend
        result = OpenStruct.new
        result.workload = plans.schema.workload
        result.workload.mix = options[:mix].to_sym \
          unless options[:mix] == 'default' && result.workload.mix != :default
        result.indexes = plans.schema.indexes.values
        backend = get_backend(options, result)

        # Get sample index values to use in queries
        index_values = index_values plans.schema.indexes.values, backend,
                                    options[:num_iterations],
                                    options[:fail_on_empty]

        total = 0
        plans.groups.each do |group, group_plans|
          next if options[:group] && group != options[:group]

          group_total = 0
          group_weight = plans.weights[group][plans.schema.workload.mix]
          next unless group_weight
          puts "Group #{group} * #{group_weight}"

          group_plans.each do |plan|
            next if options[:plan] && plan.name != options[:plan]

            update = !plan.steps.last.is_a?(Plans::IndexLookupPlanStep)
            if update
              avg = bench_update backend, plans.schema.indexes.values, plan,
                                 index_values, options[:num_iterations]
            else
              # Run the query and get the total time
              avg = bench_query backend, plans.schema.indexes.values, plan,
                                index_values, options[:num_iterations]
            end

            # Run the query and get the total time
            group_total += avg
            puts "  #{plan.name} executed in #{avg} average"
          end

          group_total *= group_weight
          total += group_total
          puts "  TOTAL #{group_total}\n"
        end

        puts "\nTOTAL #{total}"
      end

      private

      # Get the average execution time for a single query plan
      def bench_query(backend, indexes, plan, index_values, iterations)
        # Construct a list of values to be substituted in the plan
        condition_list = 1.upto(iterations).map do |i|
          Hash[plan.params.map do |field_id, condition|
            value = indexes.each do |index|
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

        prepared = backend.prepare_query nil, plan.select_fields, plan.params,
                                         [plan.steps]

        # Execute each plan and measure the time
        start_time = Time.now
        condition_list.each { |conditions| prepared.execute conditions }
        elapsed = Time.now - start_time
        avg = elapsed / iterations

        avg
      end

      # Get the average execution time for a single update plan
      def bench_update(backend, indexes, plan, index_values, iterations)
        setting_list = 1.upto(iterations).map do
          plan.update_steps.last.fields.map do |field|
            # Get the backend to generate a random ID or take a random value
            if field.is_a?(Fields::IDField)
              value = backend.generate_id
            else
              value = field.random_value
            end

            FieldSetting.new(field, value)
          end
        end

        condition_list = 1.upto(iterations).map do |i|
          Hash[plan.params.map do |field_id, condition|
            value = indexes.each do |index|
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

        prepared = backend.prepare_update nil, setting_list.first, [plan]

        # Execute each plan and measure the time
        start_time = Time.now
        setting_list.zip(condition_list).each do |settings, conditions|
          prepared.first.execute settings, conditions
        end
        elapsed = Time.now - start_time
        avg = elapsed / iterations

        avg
      end
    end
  end
end
