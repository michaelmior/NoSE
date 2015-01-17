require_relative 'search/constraints'
require_relative 'search/problem'

require 'logging'
require 'ostruct'
require 'tempfile'

module NoSE::Search
  # Searches for the optimal indices for a given workload
  class Search
    def initialize(workload)
      @logger = Logging.logger['nose::search']
      @workload = workload
    end

    # Search for optimal indices using an ILP which searches for
    # non-overlapping indices
    # @return [Array<Index>]
    def search_overlap(indexes, max_space = Float::INFINITY)
      index_sizes = indexes.map(&:size)
      return [] if indexes.empty?

      # Get the cost of all queries
      costs = costs indexes

      @logger.debug do
        "Costs: \n" + pp_s(costs) + "\n" \
        "Search with queries:\n" + \
        @workload.queries.each_with_index.map do |query, i|
          "#{i} #{query.inspect}"
        end.join("\n")
      end

      # Solve the LP using Gurobi
      solve_gurobi indexes,
                   max_space: max_space,
                   index_sizes: index_sizes,
                   costs: costs
    end

    private

    # Solve the index selection problem using Gurobi
    def solve_gurobi(indexes, data)
      # Construct and solve the ILP
      problem = Problem.new @workload, indexes, data
      problem.solve

      # We won't get here if there's no valdi solution
      @logger.debug "Found solution with total cost #{problem.objective_value}"

      # Return the selected indices
      selected_indexes = problem.selected_indexes

      @logger.debug do
        "Selected indexes:\n" + selected_indexes.map do |index|
          "#{indexes.index index} #{index.inspect}"
        end.join("\n")
      end

      selected_indexes
    end

    # Get the cost of using each index for each query in a workload
    def costs(indexes)
      planner = NoSE::Plans::QueryPlanner.new @workload, indexes

      # Create a hash to allow efficient lookup of the numerical index
      # of a particular index within the array of indexes
      index_pos = Hash[indexes.each_with_index.to_a]

      costs = Parallel.map(@workload.query_weights) do |query, weight|
        query_cost planner, index_pos, query, weight
      end

      costs
    end

    # Get the cost for indices for an individual query
    def query_cost(planner, index_pos, query, weight)
      query_costs = {}

      planner.find_plans_for_query(query).each do |plan|
        steps_by_index = []
        plan.each do |step|
          if step.is_a? NoSE::Plans::IndexLookupPlanStep
            # If one of two adjacent steps is just a lookup on
            # a single entity, we should bundle them together
            last_step = steps_by_index.last.last unless steps_by_index.empty?
            if last_step.is_a?(NoSE::Plans::IndexLookupPlanStep) &&
               (step.index.path == [last_step.index.path.last] ||
                last_step.index.path == [step.index.path.first])
              steps_by_index.last.push step
              next
            end

            steps_by_index.push [step]
          else
            steps_by_index.last.push step
          end
        end

        populate_query_costs query_costs, index_pos, steps_by_index, weight, plan
      end

      query_costs
    end

    # Store the costs and indexes for this plan in a nested hash
    def populate_query_costs(query_costs, index_pos, steps_by_index, weight, plan)
      # The first key is the number of the query and the second is the
      # number of the index
      #
      # The value is a two-element array with the numbers of the indices
      # which are jointly used to answer a step in the query plan along
      # with the cost of all plan steps for the part of the query path
      steps_by_index.each do |steps|
        step_indexes = steps.select do |step|
          step.is_a? NoSE::Plans::IndexLookupPlanStep
        end.map(&:index)
        step_indexes.map! { |index| index_pos[index] }

        cost = steps.map(&:cost).inject(0, &:+) * weight

        fail 'No more than three indexes per step' if step_indexes.length > 3

        if query_costs.key? step_indexes.first
          current_cost = query_costs[step_indexes.first].last
          current_steps = query_costs[step_indexes.first].first

          if step_indexes.length == 1 && (current_steps != step_indexes ||
                                          current_cost != cost)
            # # We should only have one step if there exists a step with length 1
            # fail 'Invalid query plan found when calculating cost: ' +
            #   plan.inspect.to_s

            # XXX This is wrong, but the costs are broken for now
            query_costs[step_indexes.first] = [step_indexes,
                                               [cost, current_cost].max]
          else
            # Take the minimum cost index for the second step
            if current_steps.length > 1
              if cost < current_cost
                query_costs[step_indexes.first] = [step_indexes, cost]
              elsif cost == current_cost
                # Cost is the same, so keep track of multiple possible last
                # steps by making the second element an array
                if current_steps.last.is_a? Array
                  new_steps = current_steps.clone
                  new_steps.last.push step_indexes.last
                  step_indexes = new_steps
                else
                  step_indexes = [step_indexes.first,
                                  [current_steps.last, step_indexes.last]]
                end
                query_costs[step_indexes.first] = [step_indexes, cost]
              end
            end
          end
        else
          query_costs[step_indexes.first] = [step_indexes, cost]
        end
      end
    end
  end
end
