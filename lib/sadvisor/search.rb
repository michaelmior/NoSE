begin
  require 'gurobi'
rescue LoadError
  # We can't use most search functionality, but it won't explode
  nil
end

require 'logging'
require 'ostruct'
require 'tempfile'

module Sadvisor
  # Searches for the optimal indices for a given workload
  class Search
    def initialize(workload)
      @logger = Logging.logger['sadvisor::search']

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

    # Write a model to a temporary file and log the file name
    def log_model(model, type, extension)
      @logger.debug do
        tmpfile = Tempfile.new ['model', extension]
        ObjectSpace.undefine_finalizer tmpfile
        model.write(tmpfile.path)
        "#{type} written to #{tmpfile.path}"
      end
    end

    # Add all necessary constraints to the Gurobi model
    def gurobi_add_constraints(model, index_vars, query_vars, indexes, data)
      # Add constraint for indices being present
      (0...indexes.length).each do |i|
        (0...@workload.queries.length).each do |q|
          model.addConstr query_vars[i][q] + index_vars[i] * -1 <= 0,
                          "i#{i}q#{q}_avail"
        end
      end

      # Add space constraint if needed
      if data[:max_space].finite?
        space = indexes.each_with_index.map do |index, i|
          index_vars[i] * (index.size * 1.0)
        end.reduce(&:+)
        model.addConstr space <= data[:max_space] * 1.0, 'max_space'
      end

      # Add complete query plan constraints
      @workload.queries.each_with_index do |query, q|
        entities = query.longest_entity_path
        query_constraint = Array.new(entities.length) do |_|
          Gurobi::LinExpr.new
        end
        data[:costs][q].each do |i, (step_indexes, _)|
          indexes[i].entity_range(entities).each do |part|
            index_var = query_vars[i][q]
            query_constraint[part] += index_var
          end

          # All indices used at this step must either all be used, or none used
          if step_indexes.length > 1
            vars = step_indexes.map { |index| query_vars[index][q] }
            model.addConstr(vars.last * 1 >= vars.first * 1)
          end
        end

        # Ensure we have exactly one index on each component of the query path
        query_constraint.each do |constraint|
          model.addConstr(constraint == 1)
        end
      end

      @logger.debug do
        model.update
        "Added #{model.getConstrs.count} constraints to model"
      end
    end

    # Set the objective function on the Gurobi model
    def gurobi_set_objective(model, query_vars, costs)
      min_cost = (0...query_vars.length).to_a \
        .product((0...@workload.queries.length).to_a).map do |i, q|
        next if costs[q][i].nil?
        query_vars[i][q] * (costs[q][i].last * 1.0)
        # XXX add weight
      end.compact.reduce(&:+)

      @logger.info { "Objective function is #{min_cost.inspect}" }

      model.setObjective(min_cost, Gurobi::MINIMIZE)
    end

    # Solve the index selection problem using Gurobi
    def solve_gurobi(indexes, data)
      # Set up solver environment
      env = Gurobi::Env.new
      env.set_int Gurobi::IntParam::METHOD,
                  Gurobi::METHOD_DETERMINISTIC_CONCURRENT
      env.set_int Gurobi::IntParam::THREADS, Parallel.processor_count

      model = Gurobi::Model.new env
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

      # Final update to the model
      model.update

      # Optionally output the model to a temporary file
      log_model model, 'Model', '.lp'

      # Run the optimization
      model.optimize

      # Ensure we found a valid solution
      status = model.get_int(Gurobi::IntAttr::STATUS)
      if status == Gurobi::INFEASIBLE
        model.computeIIS
        log_model model, 'IIS', '.ilp'
      end

      fail NoSolutionException if status != Gurobi::OPTIMAL

      obj_val = model.get_double(Gurobi::DoubleAttr::OBJ_VAL)
      @logger.debug "Found solution with total cost #{obj_val}"

      # Return the selected indices
      selected_indexes = indexes.select.with_index do |_, i|
        # Even though we specifed the variables as binary, rounding
        # error means the value won't be exactly one
        # (the check exists to catch weird values if they arise)
        val = index_vars[i].get_double(Gurobi::DoubleAttr::X)
        fail if (val > 0.01 && val < 0.99)
        val > 0.99
      end

      @logger.debug do
        "Selected indexes:\n" + selected_indexes.map do |index|
          "#{indexes.index index} #{index.inspect}"
        end.join("\n")
      end

      selected_indexes
    end

    # Get the cost of using each index for each query in a workload
    def costs(indexes)
      planner = Planner.new @workload, indexes

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
          if step.is_a? IndexLookupPlanStep
            # If the current step is just a lookup on a single entity,
            # then we should bundle it together with the last step
            last_step = steps_by_index.last.last unless steps_by_index.empty?
            if last_step.is_a?(IndexLookupPlanStep) &&
              step.index.path == [last_step.index.path.last]
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
          step.is_a? IndexLookupPlanStep
        end.map(&:index)
        step_indexes.map! { |index| index_pos[index] }

        cost = steps.map(&:cost).inject(0, &:+) * weight

        fail 'No more than two indexes per step' if step_indexes.length > 2

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
            if current_steps.length > 1 && cost < current_cost
              query_costs[step_indexes.first] = [step_indexes, cost]
            end
          end
        else
          query_costs[step_indexes.first] = [step_indexes, cost]
        end
      end
    end
  end

  # Thrown when no solution can be found to the ILP
  class NoSolutionException < StandardError
  end
end

# Only do this if Gurobi is available
if Object.const_defined?('Gurobi')
  Gurobi::LinExpr.class_eval do
    def inspect
      0.upto(size - 1).map do |i|
        coeff = getCoeff(i)
        var = getVar(i).get_string(Gurobi::StringAttr::VAR_NAME)
        "#{coeff} * #{var}"
      end.join ' + '
    end
  end
end
