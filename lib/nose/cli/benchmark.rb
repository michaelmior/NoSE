require 'csv'
require 'table_print'

# Make use the table_print gem to print in CSV format
class CSVPrint < TablePrint::Printer
  # Print the table in CSV format
  def csv_print
    # Copied from TablePrint#table_print
    group = TablePrint::RowGroup.new
    columns.each { |c| group.set_column(c) }
    group_data = (@data.first.is_a?(Hash) || @data.first.is_a?(Struct)) ? [@data] : @data
    group_data.each do |data|
      group.add_children(TablePrint::Fingerprinter.new.lift(columns, data))
    end
    group.collapse!

    # turn everything into a string for output
    CSV.generate do |csv|
      csv << group.columns.map(&:name)
      group.children.flat_map(&:children).each do |subgroup|
        top_row = subgroup.children.first.parent.parent.cells
        csv << group.columns.map { |c| top_row[c.name] }
        subgroup.children.each do |row|
          row = top_row.merge(row.cells)
          csv << group.columns.map { |c| row[c.name] }
        end
      end
    end
  end
end

module NoSE
  module CLI
    # Run performance tests on plans for a particular schema
    class NoSECLI < Thor
      desc 'benchmark PLAN_FILE', 'test performance of plans in PLAN_FILE'
      option :num_iterations, type: :numeric, default: 100
      option :repeat, type: :numeric, default: 1
      option :mix, type: :string, default: nil
      option :group, type: :string, default: nil, aliases: '-g'
      option :fail_on_empty, type: :boolean, default: true
      option :format, type: :string, default: 'txt',
                      enum: %w(txt csv), aliases: '-f'
      def benchmark(plan_file)
        result = load_results plan_file

        # Set the mix if specified, otherwise use the mix from PLAN_FILE
        result.workload.mix = options[:mix].to_sym unless options[:mix].nil?

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

        output_table table, options[:format]
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
