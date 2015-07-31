module NoSE
  module CLI
    # Run performance tests on plans for a particular schema
    class NoSECLI < Thor
      desc 'benchmark PLAN_FILE', 'test performance of plans in PLAN_FILE'
      option :num_iterations, type: :numeric, default: 100
      option :fail_on_empty, type: :boolean, default: true
      def benchmark(plan_file)
        result = load_results plan_file
        backend = get_backend(options, result)

        index_values = index_values result.indexes, backend,
                                    options[:num_iterations],
                                    options[:fail_on_empty]

        result.workload.queries.each do |query|
          @logger.debug { "Executing #{query.text}" }

          # Get the plan and indexes used for this query
          plan = result.plans.find do |possible_plan|
            possible_plan.query == query
          end
          indexes = plan.select do |step|
            step.is_a? Plans::IndexLookupPlanStep
          end.map(&:index)

          avg = bench_query backend, indexes, plan, index_values,
                            options[:num_iterations]

          # Report the time taken
          puts "#{query.text} executed in #{avg} average"
        end

        result.workload.updates.each do |update|
          @logger.debug { "Executing #{update.text}" }

          plans = result.update_plans.select do |possible_plan|
            possible_plan.statement == update
          end

          plans.each do |plan|
            # Get all indexes used by support queries
            indexes = plan.query_plans.map do |query_plan|
              query_plan.select do |step|
                step.is_a? Plans::IndexLookupPlanStep
              end.map(&:index)
            end.flatten(1)

            avg = bench_update backend, indexes, plan, index_values,
                               options[:num_iterations]

            # Report the time taken
            puts "#{update.text} executed in #{avg} average"
          end
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
