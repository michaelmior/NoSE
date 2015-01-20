require 'formatador'
require 'ostruct'
require 'json'

module NoSE::CLI
  # Add a command to run the advisor for a given workload
  class NoSECLI < Thor
    desc 'workload NAME', 'run the workload NAME'
    option :max_space, type: :numeric, default: Float::INFINITY
    option :format, type: :string, default: 'text'
    def workload(name)
      # rubocop:disable GlobalVars
      require_relative "../../../workloads/#{name}"
      workload = $workload
      # rubocop:enable GlobalVars

      enumerated_indexes = NoSE::IndexEnumerator.new(workload) \
        .indexes_for_workload.to_a

      if options[:max_space].finite?
        indexes = NoSE::Search::Search.new(workload) \
          .search_overlap(enumerated_indexes, options[:max_space])
      else
        indexes = enumerated_indexes.clone
      end

      # Find the final plans for each query
      planner = NoSE::Plans::QueryPlanner.new workload, indexes
      plans = {}
      workload.queries.map do |query|
        plans[query] = planner.min_plan query
      end

      # Get the indexes which are actually used
      indexes = plans.map(&:to_a).flatten.select do |step|
        step.is_a? NoSE::Plans::IndexLookupPlanStep
      end.map(&:index).to_set

      result = OpenStruct.new(
        workload: workload,
        enumerated_indexes: enumerated_indexes,
        indexes: indexes.to_set,
        plans: plans.values,
        total_size: indexes.map(&:size).inject(0, :+),
        total_cost: workload.statement_weights.map do |statement, weight|
          weight * plans[statement].cost
        end.inject(0, &:+)
      )

      output_json result if options[:format] == 'json'
      output_text result if options[:format] == 'text'
    end

    private

    # Output the results of advising as text
    def output_text(result)
      # Output selected indexes
      header = "Indexes\n" + '━' * 50
      Formatador.display_line "[blue]#{header}[/]"
      result.indexes.each do |index|
        puts index.inspect
      end

      Formatador.display_line "Total size: [blue]#{result.total_size}[/]\n"
      puts

      # Output queries plans for the discovered indices
      header = "Query plans\n" + '━' * 50
      Formatador.display_line "[blue]#{header}[/]"
      result.plans.each do |plan|
        puts plan.query.inspect
        puts plan.inspect
        puts
      end
    end

    # Output the results of advising as JSON
    def output_json(result)
      puts JSON.pretty_generate \
        NoSE::Serialize::SearchResultRepresenter.represent(result).to_hash
    end
  end
end
