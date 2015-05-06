require 'forwardable'
require 'ostruct'

module NoSE::Plans
  # A query planner which can construct a tree of query plans
  class QueryPlanner
    def initialize(model, indexes, cost_model)
      @logger = Logging.logger['nose::query_planner']

      @model = model
      @indexes = indexes
      @cost_model = cost_model
    end

    # Find a tree of plans for the given query
    # @return [QueryPlanTree]
    # @raise [NoPlanException]
    def find_plans_for_query(query)
      state = StatementState.new query, @model
      state.freeze
      tree = QueryPlanTree.new state, @cost_model

      # Limit indices to those which cross the query path
      entities = query.longest_entity_path
      indexes = @indexes.clone.select do |index|
        index.entity_range(entities) != (nil..nil)
      end

      indexes_by_path = indexes.to_set.group_by { |index| index.path.first }
      find_plans_for_step tree.root, indexes_by_path

      if tree.root.children.empty?
        tree = QueryPlanTree.new state, @cost_model
        find_plans_for_step tree.root, indexes_by_path, prune: false
        fail NoPlanException, "#{query.inspect} #{tree.inspect}"
      end

      @logger.debug { "Plans for #{query.inspect}: #{tree.inspect}" }

      tree
    end

    # Get the minimum cost plan for executing this query
    # @return [StatementPlan]
    def min_plan(query)
      find_plans_for_query(query).min
    end

    private

    # Remove plans ending with this step in the tree
    # @return[Boolean] true if pruning resulted in an empty tree
    def prune_plan(prune_step)
      # Walk up the tree and remove the branch for the failed plan
      while prune_step.children.length <= 1 && !prune_step.is_a?(RootPlanStep)
        prune_step = prune_step.parent
        prev_step = prune_step
      end

      # If we reached the root, we have no plan
      return true if prune_step.is_a? RootPlanStep

      prune_step.children.delete prev_step

      false
    end

    # Find possible query plans for a query strating at the given step
    def find_plans_for_step(step, indexes_by_path, used_indexes = Set.new,
                            prune: true)
      return if step.state.answered?

      steps = find_steps_for_state step, step.state,
                                   indexes_by_path, used_indexes

      if steps.length > 0
        step.children = steps
        new_used = used_indexes.clone
        steps.each do |child_step|
          find_plans_for_step child_step, indexes_by_path, new_used

          # Remove this step if finding a plan from here failed
          if child_step.children.length == 0 && !child_step.state.answered?
            step.children.delete child_step
          end
        end
      else
        if prune
          return if step.is_a?(RootPlanStep) || prune_plan(step.parent)
        else
          step.children = [PrunedPlanStep.new]
        end
      end
    end

    # Find all possible plan steps not using indexes
    # @return [Array<PlanStep>]
    def find_nonindexed_steps(parent, state)
      steps = []
      return steps if parent.is_a? RootPlanStep

      [SortPlanStep, FilterPlanStep, LimitPlanStep].each \
        { |step| steps.push step.apply(parent, state) }
      steps.flatten!
      steps.compact!

      steps
    end

    # Get a list of possible next steps for a query in the given state
    # @return [Array<PlanStep>]
    def find_steps_for_state(parent, state, indexes_by_path, used_indexes)
      steps = find_nonindexed_steps parent, state
      return steps if steps.length > 0

      # Don't allow indices to be used multiple times
      indexes = (indexes_by_path[state.path.first] || Set.new).to_set
      (indexes - used_indexes).each do |index|
        new_step = IndexLookupPlanStep.apply parent, index, state
        unless new_step.nil?
          new_step.add_fields_from_index index
          used_indexes << index
          steps.push new_step
        end
      end
      steps
    end
  end
end
