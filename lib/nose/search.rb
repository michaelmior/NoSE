require_relative 'search/constraints'
require_relative 'search/problem'
require_relative 'search/results'

require 'logging'
require 'ostruct'
require 'tempfile'

module NoSE
  # ILP construction and schema search
  module Search
    # Searches for the optimal indices for a given workload
    class Search
      def initialize(workload, cost_model)
        @logger = Logging.logger['nose::search']
        @workload = workload
        @cost_model = cost_model
      end

      # Search for optimal indices using an ILP which searches for
      # non-overlapping indices
      # @return [Array<Index>]
      def search_overlap(indexes, max_space = Float::INFINITY)
        index_sizes = indexes.map(&:size)
        return if indexes.empty?

        # Get the cost of all queries
        query_weights = Hash[@workload.support_queries(indexes).map do |query|
          [query, @workload.statement_weights[query.statement]]
        end]
        query_weights.merge!(@workload.statement_weights.select do |stmt, _|
          stmt.is_a? Query
        end.to_h)

        costs, trees = query_costs query_weights, indexes

        planner = Plans::UpdatePlanner.new @workload.model, trees, @cost_model
        update_costs = Hash.new { |h, k| h[k] = Hash.new }
        update_plans = Hash.new { |h, k| h[k] = [] }
        @workload.statements.each do |statement|
          next if statement.is_a? Query

          planner.find_plans_for_update(statement, indexes).each do |plan|
            weight = @workload.statement_weights[statement]
            update_costs[statement][plan.index] = plan.update_cost * weight
            update_plans[statement] << plan
          end
        end

        @logger.debug do
          "Costs: \n" + pp_s(costs) + "\n" \
          "Search with queries:\n" + \
            query_weights.keys.each_with_index.map do |query, i|
              "#{i} #{query.inspect}"
            end.join("\n")
        end

        # Solve the LP using Gurobi
        result = solve_gurobi query_weights.keys, indexes,
                              max_space: max_space,
                              index_sizes: index_sizes,
                              costs: costs,
                              update_costs: update_costs,
                              cost_model: @cost_model

        # Select the relevant update plans
        update_plans = update_plans.values.flatten(1).select do |plan|
          result.indexes.include? plan.index
        end
        update_plans.each do |plan|
          plan.select_query_plans result.indexes
        end
        result.update_plans = update_plans

        result.workload = @workload
        result.plans = select_plans trees, result.indexes
        result.cost_model = @cost_model

        result.validate
        result
      end

      private

      # Select the plans to use for a given set of indexes
      def select_plans(trees, indexes)
        trees.map do |tree|
          # Exclude support queries since they will be in update plans
          query = tree.query
          next if query.is_a?(SupportQuery)

          # Select the exact plan to use for these indexes
          tree.select_using_indexes(indexes).min_by(&:cost)
        end.compact
      end

      # Solve the index selection problem using Gurobi
      def solve_gurobi(queries, indexes, data)
        # Construct and solve the ILP
        problem = Problem.new queries, @workload.updates, indexes, data
        problem.solve

        # We won't get here if there's no valdi solution
        @logger.debug 'Found solution with total cost ' \
                      "#{problem.objective_value}"

        # Return the selected indices
        selected_indexes = problem.selected_indexes

        @logger.debug do
          "Selected indexes:\n" + selected_indexes.map do |index|
            "#{indexes.index index} #{index.inspect}"
          end.join("\n")
        end

        problem.result
      end

      # Get the cost of using each index for each query in a workload
      def query_costs(query_weights, indexes)
        planner = Plans::QueryPlanner.new @workload, indexes, @cost_model

        results = Parallel.map(query_weights) do |query, weight|
          query_cost planner, query, weight
        end
        costs = Hash[query_weights.keys.each_with_index.map do |query, q|
          [query, results[q].first]
        end]

        [costs, results.map(&:last)]
      end

      # Get the cost for indices for an individual query
      def query_cost(planner, query, weight)
        query_costs = {}

        tree = planner.find_plans_for_query(query)
        tree.each do |plan|
          steps_by_index = []
          plan.each do |step|
            if step.is_a? Plans::IndexLookupPlanStep
              # If one of two adjacent steps is just a lookup on
              # a single entity, we should bundle them together
              # This happens when the final step in a query plan
              # is a lookup of extra fields for an entity
              last_step = steps_by_index.last.last unless steps_by_index.empty?
              if last_step.is_a?(Plans::IndexLookupPlanStep) &&
                 (last_step.index.path.entities ==
                  [step.index.path.entities.first])
                steps_by_index.last.push step
              else
                steps_by_index.push [step]
              end
            else
              steps_by_index.last.push step
            end
          end

          populate_query_costs query_costs, steps_by_index, weight, plan
        end

        [query_costs, tree]
      end

      # Store the costs and indexes for this plan in a nested hash
      def populate_query_costs(query_costs, steps_by_index, weight, plan)
        # The first key is the query and the second is the index
        #
        # The value is a two-element array with the indices which are
        # jointly used to answer a step in the query plan along with
        # the cost of all plan steps for the part of the query path
        steps_by_index.each do |steps|
          # Get the indexes for these plan steps
          step_indexes = steps.select do |step|
            step.is_a? Plans::IndexLookupPlanStep
          end.map(&:index)

          # Calculate the cost for just these steps in the plan
          cost = steps.map do |step|
            step.cost @cost_model
          end.inject(0, &:+) * weight

          fail 'No more than two indexes per step' if step_indexes.length > 2

          if query_costs.key? step_indexes.first
            current_cost = query_costs[step_indexes.first].last
            current_steps = query_costs[step_indexes.first].first

            # We must always have the same number of steps
            fail if current_steps.length != step_indexes.length

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
          else
            query_costs[step_indexes.first] = [step_indexes, cost]
          end
        end
      end
    end
  end
end
