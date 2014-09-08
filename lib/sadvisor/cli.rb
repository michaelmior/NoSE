require 'json'
require 'ostruct'
require 'thor'

module Sadvisor
  class SadvisorCLI < Thor
    desc 'workload NAME', 'run the workload NAME'
    option :max_space, type: :numeric, default: Float::INFINITY
    option :format, type: :string, default: 'text'
    def workload(name)
      # rubocop:disable GlobalVars

      is_text = options[:format] == 'text'

      require 'sadvisor'
      require_relative "../../workloads/#{name}"

      if options[:max_space].finite?
        indexes = Sadvisor::Search.new($workload) \
          .search_overlap(options[:max_space])
      else
        # No need to search here, we'll prune to the used indices later
        indexes = Sadvisor::IndexEnumerator.new($workload) \
          .indexes_for_workload.to_a
      end

      simple_indexes = $workload.entities.values.map(&:simple_index)
      planner = Sadvisor::Planner.new $workload, (indexes + simple_indexes)
      plans = {}
      $workload.queries.each do |query|
        plans[query] = planner.min_plan query
      end

      indexes = plans.values.map(&:to_a).flatten.select do |step|
        step.is_a? Sadvisor::IndexLookupStep
      end.map(&:index).to_set

      header = "Indexes\n" + '━' * 50
      puts $stdout.isatty ? header.blue : header if is_text
      (simple_indexes.to_set + indexes).each do |index|
        puts index.inspect
      end if is_text
      puts if is_text

      total_size = (indexes - simple_indexes).map(&:size).inject(0, :+)
      puts ($stdout.isatty ? 'Total size: '.blue : 'Total size: ') + \
        total_size.to_s if is_text
      puts if is_text

      # Output queries plans for the discovered indices
      header = "Query plans\n" + '━' * 50
      puts $stdout.isatty ? header.blue : header if is_text
      plans.each do |query, plan|
        puts query.inspect if is_text
        puts plan.inspect if is_text
        puts if is_text
      end

      if options[:format] == 'json'
        result = OpenStruct.new(
          workload: $workload,
          indexes: indexes - simple_indexes,
          plans: plans.values,
          total_size: total_size,
          total_cost: $workload.query_weights.map do |query, weight|
            weight * plans[query].cost
          end.inject(0, &:+)
        )

        puts JSON.pretty_generate \
          Sadvisor::SearchResultRepresenter.represent(result).to_hash
      end

      # rubocop:enable GlobalVars
    end
  end
end
