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
      def initialize(workload, cost_model, objective = Objective::COST)
        @logger = Logging.logger['nose::search']
        @workload = workload
        @cost_model = cost_model
        @objective = objective
      end

      # Search for optimal indices using an ILP which searches for
      # non-overlapping indices
      # @return [Array<Index>]
      def search_overlap(indexes, max_space = Float::INFINITY)
        return if indexes.empty?

        # Get the costs of all queries and updates
        query_weights = combine_query_weights indexes
        costs, trees = query_costs query_weights, indexes
        update_costs, update_plans = update_costs trees, indexes

        log_search_start costs, query_weights

        solver_params = {
          max_space: max_space,
          costs: costs,
          update_costs: update_costs,
          cost_model: @cost_model
        }
        search_result query_weights, indexes, solver_params, trees,
                      update_plans
      end

      private

      # Combine the weights of queries and statements
      def combine_query_weights(indexes)
        query_weights = Hash[@workload.support_queries(indexes).map do |query|
          [query, @workload.statement_weights[query.statement]]
        end]
        query_weights.merge!(@workload.statement_weights.select do |stmt, _|
          stmt.is_a? Query
        end.to_h)

        query_weights
      end

      # Produce a useful log message before starting the search
      def log_search_start(costs, query_weights)
        @logger.debug do
          "Costs: \n" + pp_s(costs) + "\n" \
            "Search with queries:\n" + \
            query_weights.each_key.each_with_index.map do |query, i|
              "#{i} #{query.inspect}"
            end.join("\n")
        end
      end

      # Run the solver and get the results of search
      def search_result(query_weights, indexes, solver_params, trees,
                        update_plans)
        # Solve the LP using MIPPeR
        result = solve_mipper query_weights.keys, indexes, **solver_params

        result.workload = @workload
        result.plans_from_trees trees
        result.cost_model = @cost_model

        # Select the relevant update plans
        update_plans = update_plans.values.flatten(1).select do |plan|
          result.indexes.include? plan.index
        end
        update_plans.each do |plan|
          plan.select_query_plans(&result.method(:select_plan))
        end
        result.update_plans = update_plans

        result.validate

        result
      end

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

      # Solve the index selection problem using MIPPeR
      def solve_mipper(queries, indexes, data)
        # Construct and solve the ILP
        problem = Problem.new queries, @workload.updates, indexes, data,
                              @objective
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

      # Produce the cost of updates in the workload
      def update_costs(trees, indexes)
        planner = Plans::UpdatePlanner.new @workload.model, trees, @cost_model
        update_costs = Hash.new { |h, k| h[k] = {} }
        update_plans = Hash.new { |h, k| h[k] = [] }
        @workload.statements.each do |statement|
          next if statement.is_a? Query

          populate_update_costs planner, statement, indexes,
                                update_costs, update_plans
        end

        [update_costs, update_plans]
      end

      # Populate the cost of all necessary plans for the given satement
      def populate_update_costs(planner, statement, indexes,
                                update_costs, update_plans)
        planner.find_plans_for_update(statement, indexes).each do |plan|
          weight = @workload.statement_weights[statement]
          update_costs[statement][plan.index] = plan.update_cost * weight
          update_plans[statement] << plan
        end
      end

      # Get the cost of using each index for each query in a workload
      def query_costs(query_weights, indexes)
        planner = Plans::QueryPlanner.new @workload, indexes, @cost_model

        results = Parallel.map(query_weights) do |query, weight|
          query_cost planner, query, weight
        end
        costs = Hash[query_weights.each_key.each_with_index.map do |query, q|
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
              steps_by_index.push [step]
            else
              steps_by_index.last.push step
            end
          end

          populate_query_costs query_costs, steps_by_index, weight
        end

        [query_costs, tree]
      end

      # Store the costs and indexes for this plan in a nested hash
      def populate_query_costs(query_costs, steps_by_index, weight)
        # The first key is the query and the second is the index
        #
        # The value is a two-element array with the indices which are
        # jointly used to answer a step in the query plan along with
        # the cost of all plan steps for the part of the query path
        steps_by_index.each do |steps|
          # Get the indexes for these plan steps
          index_step = steps.first

          # Calculate the cost for just these steps in the plan
          cost = steps.sum_by(&:cost) * weight

          if query_costs.key? index_step.index
            current_cost = query_costs[index_step.index].last

            # We must always have the same cost
            fail if current_cost != cost
          else
            # We either found a new plan or something cheaper
            query_costs[index_step.index] = [steps, cost]
          end
        end
      end
    end
  end
end
