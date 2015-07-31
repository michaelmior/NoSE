module NoSE
  module CLI
    # Run performance tests on plans for a particular schema
    class NoSECLI < Thor
      desc 'benchmark PLAN_FILE', 'test performance of plans in PLAN_FILE'
      option :num_iterations, type: :numeric, default: 100
      def benchmark(plan_file)
        result = load_results plan_file
        backend = get_backend(options, result)

        index_values = index_values result.indexes, backend,
                                    options[:num_iterations]

        result.workload.queries.each do |query|
          # Get the plan and indexes used for this query
          plan = result.plans.find do |possible_plan|
            possible_plan.query == query
          end
          indexes = plan.select do |step|
            step.is_a? Plans::IndexLookupPlanStep
          end.map(&:index)

          # Construct a prepared statement for the query
          prepared = backend.prepare(query, [plan])

          # Construct a list of values to be substituted in the query
          condition_list = 1.upto(options[:num_iterations]).map do |i|
            Hash[query.conditions.map do |field_id, condition|
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

          # Execute each query and measure the time
          start_time = Time.now
          condition_list.each { |conditions| prepared.execute conditions }
          elapsed = Time.now - start_time

          # Report the time taken
          puts "#{query.text} executed in " \
               "#{elapsed / options[:num_iterations]} average"
        end
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
