require 'forwardable'
require 'ostruct'

module NoSE
  module Plans
    # Ongoing state of a query throughout the execution plan
    class QueryState
      attr_accessor :from, :fields, :eq, :range, :order_by, :path,
                    :cardinality, :hash_cardinality, :given_fields
      attr_reader :query, :model

      def initialize(query, model)
        @query = query
        @model = model
        @from = query.from
        @fields = query.select
        @eq = query.eq_fields.dup
        @range = query.range_field
        @path = query.key_path.reverse
        @order_by = query.order.dup

        # We never need to order by fields listed in equality predicates
        # since we'll only ever have rows with a single value
        @order_by -= @eq.to_a

        @cardinality = 1  # this will be updated on the first index lookup
        @hash_cardinality = 1
        @given_fields = @eq.dup
      end

      # All the fields referenced anywhere in the query
      def all_fields
        all_fields = @fields + @eq
        all_fields << @range unless @range.nil?
        all_fields
      end

      # :nocov:
      def to_color
        @query.text +
          "\n  fields: " + @fields.map(&:to_color).to_a.to_color +
          "\n      eq: " + @eq.map(&:to_color).to_a.to_color +
          "\n   range: " + (@range.nil? ? '(nil)' : @range.name) +
          "\n   order: " + @order_by.map(&:to_color).to_a.to_color +
          "\n    path: " + @path.to_a.to_color
      end
      # :nocov:

      # Check if the query has been fully answered
      # @return [Boolean]
      def answered?(check_limit: true)
        done = @fields.empty? && @eq.empty? && @range.nil? && @order_by.empty?

        # Check if the limit has been applied
        done &&= @cardinality <= @query.limit unless @query.limit.nil? ||
          !check_limit

        done
      end

      # Get all fields relevant for filtering in the query for entities
      # in the given list, optionally including selected fields
      # @return [Array<Field>]
      def fields_for_entities(entities, select: false)
        path_fields = @eq + @order_by

        # If necessary, include ALL the fields which should be selected,
        # otherwise we can exclude fields from the last entity set since
        # we may end up selecting these with a separate index lookup
        if select
          path_fields += @fields
        else
          path_fields += @fields.select do
            |field| entities[0..-2].include? field.parent
          end
        end

        path_fields << @range unless @range.nil?
        path_fields.select { |field| entities.include? field.parent }
      end
    end

    # A tree of possible query plans
    class QueryPlanTree
      include Enumerable

      attr_reader :root
      attr_accessor :cost_model

      def initialize(state, cost_model)
        @root = RootPlanStep.new(state)
        @cost_model = cost_model
      end

      # Select all plans which use only a given set of indexes
      def select_using_indexes(indexes)
        select do |plan|
          plan.all? do |step|
            !step.is_a?(Plans::IndexLookupPlanStep) ||
              indexes.include?(step.index)
          end
        end
      end

      # The query this tree of plans is generated for
      def query
        @root.state.query
      end

      # Enumerate all plans in the tree
      def each
        nodes = [@root]

        while nodes.length > 0
          node = nodes.pop
          if node.children.length > 0
            nodes.concat node.children.to_a
          else
            # This is just an extra check to make absolutely
            # sure we never consider invalid statement plans
            fail unless node.state.answered?

            yield node.parent_steps @cost_model
          end
        end
      end

      # Return the total number of plans for this statement
      # @return [Integer]
      def size
        to_a.count
      end

      # :nocov:
      def to_color(step = nil, indent = 0)
        step = @root if step.nil?
        this_step = '  ' * indent + step.to_color
        this_step += " [yellow]$#{step.cost(@cost_model).round 5}[/]" \
          unless step.is_a? RootPlanStep
        this_step + "\n" + step.children.map do |child_step|
          to_color child_step, indent + 1
        end.reduce('', &:+)
      end
      # :nocov:
    end

    # Thrown when it is not possible to construct a plan for a statement
    class NoPlanException < StandardError
    end

    # A single plan for a query
    class QueryPlan
      attr_accessor :query
      attr_accessor :cost_model

      include Comparable
      include Enumerable

      # Most of the work is delegated to the array
      extend Forwardable
      def_delegators :@steps, :each, :<<, :[], :==, :===, :eql?,
        :inspect, :to_s, :to_a, :to_ary, :last, :length, :count

      def initialize(query, cost_model)
        @steps = []
        @query = query
        @cost_model = cost_model
      end

      # Two plans are compared by their execution cost
      def <=>(other)
        cost <=> other.cost
      end

      # The estimated cost of executing the query using this plan
      # @return [Numeric]
      def cost
        @steps.map { |step| step.cost @cost_model }.inject(0, &:+)
      end

      # Get the indexes used by this query plan
      def indexes
        @steps.select { |step| step.is_a? IndexLookupPlanStep }.map(&:index)
      end
    end

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
        state = QueryState.new query, @model
        state.freeze
        tree = QueryPlanTree.new state, @cost_model

        # Limit indices to those which cross the query path
        entities = query.longest_entity_path
        indexes = @indexes.clone.select do |index|
          index.entity_range(entities) != (nil..nil)
        end

        indexes_by_path = indexes.to_set.group_by do |index|
          index.path.first.parent
        end
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
      # @return [QueryPlan]
      def min_plan(query)
        find_plans_for_query(query).min
      end

      private

      # Remove plans ending with this step in the tree
      # @return[Boolean] true if pruning resulted in an empty tree
      def prune_plan(prune_step)
        # Walk up the tree and remove the branch for the failed plan
        while prune_step.children.length <= 1 &&
              !prune_step.is_a?(RootPlanStep)
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
        indexes = (indexes_by_path[state.path.first.parent] || Set.new).to_set
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
end
