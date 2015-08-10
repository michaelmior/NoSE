require 'table_print'

module NoSE
  module CLI
    # Run performance tests on plans for a particular schema
    class NoSECLI < Thor
      desc 'execute PLANS', 'test performance of the named PLANS'
      option :num_iterations, type: :numeric, default: 100
      option :repeat, type: :numeric, default: 1
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

        table = []
        total = 0
        plans.groups.each do |group, group_plans|
          next if options[:group] && group != options[:group]

          group_table = []
          group_total = 0
          group_weight = plans.weights[group][plans.schema.workload.mix]
          next unless group_weight

          group_plans.each do |plan|
            next if options[:plan] && plan.name != options[:plan]

            update = !plan.steps.last.is_a?(Plans::IndexLookupPlanStep)
            if update
              measurement = bench_update backend, plans.schema.indexes.values,
                                         plan, index_values,
                                         options[:num_iterations],
                                         options[:repeat], weight: group_weight
            else
              # Run the query and get the total time
              measurement = bench_query backend, plans.schema.indexes.values,
                                        plan, index_values,
                                        options[:num_iterations],
                                        options[:repeat], weight: group_weight
            end

            # Run the query and get the total time
            group_total += measurement.mean
            group_table << measurement
          end

          total_measurement = Measurements::Measurement.new nil, 'TOTAL'
          total_measurement << group_table.map(&:weighted_mean).inject(0, &:+)
          group_table << total_measurement
          table << OpenStruct.new(group: group, measurements: group_table)
          group_total *= group_weight
          total += group_total
        end

        total_measurement = Measurements::Measurement.new nil, 'TOTAL'
        total_measurement << table.map do |group|
          group.measurements.find { |m| m.name == 'TOTAL' }.mean
        end.inject(0, &:+)
        table << OpenStruct.new(group: 'TOTAL',
                                measurements: [total_measurement])

        tp table, *[
          'group',
          { 'measurements.name' => { display_name: 'name' } },
          { 'measurements.mean' => { display_name: 'mean' } }
        ]
      end

      private

      # Get the average execution time for a single query plan
      def bench_query(backend, indexes, plan, index_values, iterations, repeat,
                      weight: weight)
        # Construct a list of values to be substituted in the plan
        condition_list = 1.upto(iterations).map do |i|
          Hash[plan.params.map do |field_id, condition|
            value = nil
            indexes.each do |index|
              values = index_values[index]
              next if values.empty?
              value = values[i % values.length][condition.field.id]
              break unless value.nil?
            end

            [
              field_id,
              Condition.new(condition.field, condition.operator, value)
            ]
          end]
        end

        prepared = backend.prepare_query nil, plan.select_fields, plan.params,
                                         [plan.steps]

        measurement = Measurements::Measurement.new plan, weight: weight

        1.upto(repeat) do
          # Execute each plan and measure the time
          start_time = Time.now
          condition_list.each { |conditions| prepared.execute conditions }
          elapsed = Time.now - start_time

          measurement << (elapsed / iterations)
        end

        measurement
      end

      # Get the average execution time for a single update plan
      def bench_update(backend, indexes, plan, index_values,
                       iterations, repeat, weight: weight)
        params = plan.params
        condition_list = 1.upto(iterations).map do |i|
          Hash[params.map do |field_id, condition|
            value = nil
            indexes.each do |index|
              values = index_values[index]
              next if values.empty?
              value = values[i % values.length][condition.field.id]
              break unless value.nil?
            end

            [
              field_id,
              Condition.new(condition.field, condition.operator, value)
            ]
          end]
        end

        # Get values for the fields which were provided as parameters
        fields = plan.update_steps.last.fields.select do |field|
          plan.params.key? field.id
        end
        setting_list = 1.upto(iterations).map do |i|
          fields.map do |field|
            # First check for IDs given as part of the query otherwise
            # get the backend to generate a random ID or take a random value
            condition = condition_list[i - 1][field.id]
            if !condition.nil? && field.is_a?(Fields::IDField)
              value = condition.value
            elsif field.is_a?(Fields::IDField)
              value = backend.generate_id
            else
              value = field.random_value
            end

            FieldSetting.new(field, value)
          end
        end

        update_settings = plan.update_steps.last.fields.map do |field|
          FieldSetting.new field, nil
        end
        prepared = backend.prepare_update nil, update_settings, [plan]

        measurement = Measurements::Measurement.new plan, weight: weight

        1.upto(repeat) do
          # Execute each plan and measure the time
          start_time = Time.now
          setting_list.zip(condition_list).each do |settings, conditions|
            prepared.first.execute settings, conditions
          end
          elapsed = Time.now - start_time

          measurement << (elapsed / iterations)
        end

        measurement
      end
    end
  end
end
