module NoSE
  module CLI
    # Run performance tests on plans for a particular schema
    class NoSECLI < Thor
      desc 'benchmark PLAN_FILE', 'test performance of plans in PLAN_FILE'
      option :num_iterations, type: :numeric, default: 100
      option :repeat, type: :numeric, default: 1
      option :fail_on_empty, type: :boolean, default: true
      def benchmark(plan_file)
        result = load_results plan_file
        backend = get_backend(options, result)

        index_values = index_values result.indexes, backend,
                                    options[:num_iterations],
                                    options[:fail_on_empty]

        group_tables = Hash.new { |h, k| h[k] = [] }
        group_totals = Hash.new { |h, k| h[k] = 0 }
        result.workload.queries.each do |query|
          weight = result.workload.statement_weights[query]
          next if query.is_a?(SupportQuery) || !weight
          @logger.debug { "Executing #{query.text}" }

          # Get the plan and indexes used for this query
          plan = result.plans.find do |possible_plan|
            possible_plan.query == query
          end
          indexes = plan.select do |step|
            step.is_a? Plans::IndexLookupPlanStep
          end.map(&:index)

          measurement = bench_query backend, indexes, plan, index_values,
                                    options[:num_iterations], options[:repeat],
                                    weight: weight

          group_totals[plan.group] += measurement.mean
          group_tables[plan.group] << measurement
        end

        result.workload.updates.each do |update|
          weight = result.workload.statement_weights[update]
          next unless weight
          @logger.debug { "Executing #{update.text}" }

          plans = result.update_plans.select do |possible_plan|
            possible_plan.statement == update
          end

          plans.each do |plan|
            # Get all indexes used by support queries
            indexes = plan.query_plans.map(&:indexes).flatten(1) << plan.index

            measurement = bench_update backend, indexes, plan, index_values,
                                       options[:num_iterations],
                                       options[:repeat], weight: weight

            group_totals[plan.group] += measurement.mean
            group_tables[plan.group] << measurement
          end
        end

        total = 0
        table = []
        group_totals.each do |group, group_total|
          total += group_total
          total_measurement = Measurements::Measurement.new nil, 'TOTAL'
          group_table = group_tables[group]
          total_measurement << group_table.map(&:weighted_mean).inject(0, &:+)
          group_table << total_measurement
          table << OpenStruct.new(group: group, measurements: group_table)
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

      # Get a sample of values from each index used by the queries
      def index_values(indexes, backend, iterations, fail_on_empty = true)
        Hash[indexes.map do |index|
          values = backend.index_sample(index, iterations).to_a
          fail "Index #{index.key} is empty and will produce no results" \
            if values.empty? && fail_on_empty

          [index, values]
        end]
      end
    end
  end
end
