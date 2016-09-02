# frozen_string_literal: true

require 'csv'
require 'table_print'

module NoSE
  module CLI
    # Run performance tests on plans for a particular schema
    class NoSECLI < Thor
      desc 'benchmark PLAN_FILE', 'test performance of plans in PLAN_FILE'

      long_desc <<-LONGDESC
        `nose benchmark` will take a JSON file output by `nose search`,
        execute each statement, and output a summary of the execution times.
        Before runnng benchmark, `nose create` and `nose load` must be used to
        prepare the target database.
      LONGDESC

      shared_option :mix

      option :num_iterations, type: :numeric, default: 100,
                              banner: 'ITERATIONS',
                              desc: 'the number of times to execute each ' \
                                    'statement'
      option :repeat, type: :numeric, default: 1,
                      desc: 'how many times to repeat the benchmark'
      option :group, type: :string, default: nil, aliases: '-g',
                     desc: 'restrict the benchmark to statements in the ' \
                           'given group'
      option :fail_on_empty, type: :boolean, default: true,
                             desc: 'abort if a column family is empty'
      option :totals, type: :boolean, default: false, aliases: '-t',
                      desc: 'whether to include group totals in the output'
      option :format, type: :string, default: 'txt',
                      enum: %w(txt csv), aliases: '-f',
                      desc: 'the format of the output data'

      def benchmark(plan_file)
        label = File.basename plan_file, '.*'
        result = load_results plan_file, options[:mix]

        backend = get_backend(options, result)

        index_values = index_values result.indexes, backend,
                                    options[:num_iterations],
                                    options[:fail_on_empty]

        group_tables = Hash.new { |h, k| h[k] = [] }
        group_totals = Hash.new { |h, k| h[k] = 0 }
        result.plans.each do |plan|
          query = plan.query
          weight = result.workload.statement_weights[query]
          next if query.is_a?(SupportQuery) || !weight
          @logger.debug { "Executing #{query.text}" }

          next unless options[:group].nil? || plan.group == options[:group]

          indexes = plan.select do |step|
            step.is_a? Plans::IndexLookupPlanStep
          end.map(&:index)

          measurement = bench_query backend, indexes, plan, index_values,
                                    options[:num_iterations], options[:repeat],
                                    weight: weight
          next if measurement.empty?

          measurement.estimate = plan.cost
          group_totals[plan.group] += measurement.mean
          group_tables[plan.group] << measurement
        end

        result.workload.updates.each do |update|
          weight = result.workload.statement_weights[update]
          next unless weight

          plans = (result.update_plans || []).select do |possible_plan|
            possible_plan.statement == update
          end
          next if plans.empty?

          @logger.debug { "Executing #{update.text}" }

          plans.each do |plan|
            next unless options[:group].nil? || plan.group == options[:group]

            # Get all indexes used by support queries
            indexes = plan.query_plans.flat_map(&:indexes) << plan.index

            measurement = bench_update backend, indexes, plan, index_values,
                                       options[:num_iterations],
                                       options[:repeat], weight: weight
            next if measurement.empty?

            measurement.estimate = plan.cost
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
          total_measurement << group_table.map(&:weighted_mean) \
                               .inject(0, &:+)
          group_table << total_measurement if options[:totals]
          table << OpenStruct.new(label: label, group: group,
                                  measurements: group_table)
        end

        if options[:totals]
          total_measurement = Measurements::Measurement.new nil, 'TOTAL'
          total_measurement << table.map do |group|
            group.measurements.find { |m| m.name == 'TOTAL' }.mean
          end.inject(0, &:+)
          table << OpenStruct.new(label: label, group: 'TOTAL',
                                  measurements: [total_measurement])
        end

        case options[:format]
        when 'txt'
          output_table table
        else
          output_csv table
        end
      end

      private

      # Get a sample of values from each index used by the queries
      # @return [Hash]
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
