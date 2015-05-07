module NoSE::CLI
  #
  class NoSECLI < Thor
    desc 'benchmark PLAN_FILE', 'test performance of plans in PLAN_FILE'
    option :num_iterations, type: :numeric, default: 100
    def benchmark(plan_file)
      result = load_results plan_file
      config = load_config
      backend = get_backend(config, result)

      index_values = index_values result.indexes, backend,
                                  options[:num_iterations]

      result.workload.queries.each do |query|
        # Get the plan and indexes used for this query
        plan = result.plans.find do |possible_plan|
          possible_plan.statement == query
        end
        indexes = plan.select do |step|
          step.is_a? NoSE::Plans::IndexLookupPlanStep
        end.map(&:index)

        # Construct a list of values to be substituted in the query
        condition_fields = query.conditions.map(&:field)
        condition_values = 1.upto(options[:num_iterations]).map do |i|
          condition_fields.map do |field|
            indexes.each do |index|
              values = index_values[index]
              value = values[i % values.length][field.id]
              break value unless value.nil?
            end
          end
        end

        # Execute each query and measure the time
        start_time = Time.now
        condition_values.each do |values|
          query.conditions.each_with_index do |condition, i|
            condition.instance_variable_set :@value, values[i]
          end
          backend.query query, plan
        end

        # Report the time taken
        elapsed = Time.now - start_time
        puts "#{query.text} executed in " \
             "#{elapsed / options[:num_iterations]} average"
      end
    end

    private

    # Get a sample of values from each index used by the queries
    def index_values(indexes, backend, iterations)
      Hash[indexes.map do |index|
        values = backend.index_sample(index, iterations).to_a
        fail "Index #{index.key} is empty and will produce no results" \
          if values.empty?

        [index, values]
      end]
    end
  end
end
