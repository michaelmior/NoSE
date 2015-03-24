require 'formatador'
require 'ostruct'
require 'json'

module NoSE::CLI
  # Add a command to run the advisor for a given workload
  class NoSECLI < Thor
    desc 'workload NAME', 'run the workload NAME'
    option :max_space, type: :numeric, default: Float::INFINITY
    option :format, type: :string, default: 'text',
                    enum: ['text', 'json', 'yaml']
    option :output, type: :string, default: nil
    def workload(name)
      # rubocop:disable GlobalVars
      require_relative "../../../workloads/#{name}"
      workload = $workload
      # rubocop:enable GlobalVars

      # Get the cost model
      config = load_config
      cost_model = get_class 'cost', config[:cost_model][:name]

      enumerated_indexes = NoSE::IndexEnumerator.new(workload) \
                           .indexes_for_workload.to_a
      indexes = find_indexes workload, enumerated_indexes, options[:max_space],
                             cost_model

      # Find the final plans for each query
      config = load_config
      cost_model = get_class 'cost', config[:cost_model][:name]
      planner = NoSE::Plans::QueryPlanner.new workload, indexes, cost_model
      plans = {}
      workload.queries.each { |query| plans[query] = planner.min_plan query }

      # Get the indexes which are actually used
      indexes = plans.map(&:to_a).flatten.select do |step|
        step.is_a? NoSE::Plans::IndexLookupPlanStep
      end.map(&:index).to_set

      result = OpenStruct.new(
        workload: workload,
        enumerated_indexes: enumerated_indexes,
        indexes: indexes.to_set,
        plans: plans.values,
        cost_model: cost_model,
        total_size: indexes.map(&:size).inject(0, :+),
        total_cost: workload.statement_weights.map do |statement, weight|
          next 0 unless statement.is_a? NoSE::Query
          weight * plans[statement].cost
        end.inject(0, &:+)
      )

      # Output the results in the specified format
      file = options[:output].nil? ? $stdout : File.open(options[:output], 'w')
      begin
        send(('output_' + options[:format]).to_sym, result, file)
      ensure
        file.close unless options[:output].nil?
      end
    end

    private

    # Find the indexes with the given space constraint
    def find_indexes(workload, enumerated_indexes, space, cost_model)
      if space.finite?
        NoSE::Search::Search.new(workload, cost_model) \
          .search_overlap enumerated_indexes, space
      else
        enumerated_indexes.clone
      end
    end
  end
end
