require 'gurobi'
require 'ostruct'
require 'tempfile'

module Sadvisor
  # Searches for the optimal indices for a given workload
  class Search
    def initialize(workload)
      @workload = workload
    end

    # Search for optimal indices using an ILP which searches for
    # non-overlapping indices
    # @return [Array<Index>]
    def search_overlap(max_space = Float::INFINITY)
      # Generate all possible combinations of indices
      simple_indexes = $workload.entities.values.map(&:simple_index)
      indexes = IndexEnumerator.new(@workload).indexes_for_workload.to_a
      indexes += simple_indexes
      index_sizes = indexes.map(&:size)
      return [] if indexes.empty?

      # Get the cost of all queries and check for overlapping indices
      costs = costs indexes
      query_overlap = overlap indexes, costs

      # Solve the LP using Gurobi
      solve_gurobi indexes,
                   max_space: max_space,
                   index_sizes: index_sizes,
                   query_overlap: query_overlap,
                   costs: costs
    end

    private

    # Add all necessary constraints to the Gurobi model
    def gurobi_add_constraints(model, index_vars, query_vars, indexes, data)
      # Add constraint for indices being present
      (0...indexes.length).each do |i|
        (0...@workload.queries.length).each do |q|
          model.addConstr(query_vars[i][q] + index_vars[i] * -1 <= 0)
        end
      end

      # Add space constraint if needed
      if data[:max_space].finite?
        space = indexes.each_with_index.map do |index, i|
          index_vars[i] * (index.size * 1.0)
        end.reduce(&:+)
        model.addConstr(space <= data[:max_space] * 1.0)
      end

      # Add overlapping index constraints
      data[:query_overlap].each do |q, overlaps|
        overlaps.each do |i, overlap|
          overlap.each do |j|
            model.addConstr(query_vars[i][q] + query_vars[j][q] <= 1)
          end
        end
      end

      # Add complete query plan constraints
      @workload.queries.each_with_index do |query, q|
        entities = query.longest_entity_path
        query_constraint = Array.new(entities.length) do |_|
          Gurobi::LinExpr.new
        end
        data[:costs][q].keys.each do |i|
          indexes[i].entity_range(entities).each do |part|
            index_var = query_vars[i][q]
            query_constraint[part] += index_var
          end
        end

        query_constraint.each do |constraint|
          model.addConstr(constraint == 1)
        end
      end
    end

    # Set the objective function on the Gurobi model
    def gurobi_set_objective(model, query_vars, costs)
      min_cost = (0...query_vars.length).to_a \
        .product((0...@workload.queries.length).to_a).map do |i, q|
        next if costs[q][i].nil?
        query_vars[i][q] * (costs[q][i] * 1.0)
      end.compact.reduce(&:+)

      model.setObjective(min_cost, Gurobi::MINIMIZE)
    end

    # Solve the index selection problem using Gurobi
    def solve_gurobi(indexes, data)
      model = Gurobi::Model.new(Gurobi::Env.new)
      model.getEnv.set_int(Gurobi::IntParam::OUTPUT_FLAG, 0)

      # Initialize query and index variables
      index_vars = []
      query_vars = []
      (0...indexes.length).each do |i|
        index_vars[i] = model.addVar(0, 1, 0, Gurobi::BINARY, "i#{i}")
        query_vars[i] = []
        (0...@workload.queries.length).each do |q|
          query_vars[i][q] = model.addVar(0, 1, 0, Gurobi::BINARY, "q#{q}i#{i}")

        end
      end

      # Add all constraints to the model
      model.update
      gurobi_add_constraints model, index_vars, query_vars, indexes, data

      # Set the objective function
      gurobi_set_objective model, query_vars, data[:costs]

      # Run the optimizer
      model.update
      model.optimize

      # Return the selected indices
      indexes.select.with_index do |_, i|
        index_vars[i].get_double(Gurobi::DoubleAttr::X) == 1.0
      end
    end

    # Determine which indices overlap each other for queries in the workload
    def overlap(indexes, costs)
      query_overlap = {}

      Parallel.each_with_index(@workload.queries) do |query, i|
        entities = query.longest_entity_path
        query_indices = costs[i].keys.each do |j|
          [j, indexes[j].entity_range(entities)]
        end

        query_indices.each_with_index do |(overlap1, range1), j|
          query_indices[j + 1..-1].each do |(overlap2, range2)|
            unless (range1.to_a & range2.to_a).empty?
              query_overlap[i] = {} unless query_overlap.key?(i)
              if query_overlap[i].key? overlap1
                query_overlap[i][overlap1] << overlap2
              else
                query_overlap[i][overlap1] = [overlap2]
              end
            end
          end
        end
      end

      query_overlap
    end

    # Get the cost of using each index for each query in a workload
    def costs(indexes)
      planner = Planner.new @workload, indexes
      costs = Array.new(@workload.queries.length) { |_| {} }

      @workload.queries.each_with_index do |query, i|
        planner.find_plans_for_query(query).each do |plan|
          steps_by_index = []
          plan.each do |step|
            if step.is_a? IndexLookupPlanStep
              steps_by_index.push [step]
            else
              steps_by_index.last.push step
            end
          end

          # Store the costs for this plan in a nested hash where
          steps_by_index.each do |steps|
            index = steps.first.index
            cost = steps.map(&:cost).inject(0, &:+)
            costs[i][indexes.index index] = cost
          end
        end
      end

      costs
    end
  end
end
