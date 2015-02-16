require 'forwardable'
require 'ostruct'

module NoSE::Plans
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
  end

  # A single step in a query plan
  class PlanStep
    include Supertype

    attr_accessor :state, :parent
    attr_reader :children, :fields

    def initialize
      @children = Set.new
      @parent = nil
      @fields = Set.new
    end

    # :nocov:
    def to_color
      # Split on capital letters and remove the last two parts (PlanStep)
      self.class.name.split('::').last.split(/(?=[A-Z])/)[0..-3] \
          .map(&:downcase).join(' ').capitalize
    end
    # :nocov:

    def children=(children)
      @children = children.to_set

      # Track the parent step of each step
      children.each do |child|
        child.instance_variable_set(:@parent, self)
        fields = child.instance_variable_get(:@fields) + self.fields
        child.instance_variable_set(:@fields, fields)
      end
    end

    # Mark the fields in this index as fetched
    def add_fields_from_index(index)
      @fields += index.all_fields
    end

    # Get the list of steps which led us here
    # If a cost model is not provided, query plans using
    # this step cannot be evaluated on the basis of cost
    #
    # (this is to support PlanStep#parent_index which does not need cost)
    # @return [QueryPlan]
    def parent_steps(cost_model = nil)
      steps = nil

      if @parent.nil?
        steps = QueryPlan.new state.query, cost_model
      else
        steps = @parent.parent_steps cost_model
        steps << self
      end

      steps
    end

    # Find the closest index to this step
    def parent_index
      step = parent_steps.to_a.reverse.find do |parent_step|
        parent_step.is_a? IndexLookupPlanStep
      end
      step.index unless step.nil?
    end

    # The cost of executing this step in the plan
    # @return [Numeric]
    def cost(cost_model)
      cost_model.method((subtype_name + '_cost').to_sym).call self
    end

    # Add the Subtype module to all step classes
    def self.inherited(child_class)
      child_class.send(:include, Subtype)
    end
  end

  # A dummy step used to inspect failed query plans
  class PrunedPlanStep < PlanStep
    def state
      OpenStruct.new answered?: true
    end
  end

  # The root of a tree of query plans used as a placeholder
  class RootPlanStep < PlanStep
    def initialize(state)
      super()
      @state = state
    end
  end

  # Ongoing state of a query throughout the execution plan
  class QueryState
    attr_accessor :from, :fields, :eq, :range, :order_by, :path, :cardinality,
                  :given_fields
    attr_reader :query, :entities, :model

    def initialize(query, model)
      @query = query
      @model = model
      @from = query.from
      @fields = query.select
      @eq = query.eq_fields
      @range = query.range_field
      @order_by = query.order
      @path = query.longest_entity_path.reverse
      @cardinality = @path.first.count
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
        "\n  fields: " + @fields.map { |field| field.to_color }.to_a.to_color +
        "\n      eq: " + @eq.map { |field| field.to_color }.to_a.to_color +
        "\n   range: " + (@range.nil? ? '(nil)' : @range.name) +
        "\n   order: " + @order_by.map do |field|
                           field.to_color
                         end.to_a.to_color +
        "\n    path: " + @path.to_a.to_color
    end
    # :nocov:

    # Check if the query has been fully answered
    # @return [Boolean]
    def answered?(check_limit: true)
      done = @fields.empty? && @eq.empty? && @range.nil? && @order_by.empty?
      done &&= @cardinality == @query.limit unless @query.limit.nil? ||
                                                   !check_limit

      done
    end

    # Get all fields relevant for filtering in the query for entities in the
    # given list, optionally including selected fields
    # @return [Array<Field>]
    def fields_for_entities(entities, select: false)
      path_fields = @eq + @order_by
      path_fields += @fields if select
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

    # Enumerate all plans in the tree
    def each
      nodes = [@root]

      while nodes.length > 0
        node = nodes.pop
        if node.children.length > 0
          nodes.concat node.children.to_a
        else
          # This is just an extra check to make absolutely
          # sure we never consider invalid query plans
          fail unless node.state.answered?

          yield node.parent_steps @cost_model
        end
      end
    end

    # Return the total number of plans for this query
    # @return [Integer]
    def size
      to_a.count
    end

    # :nocov:
    def to_color(step = nil, indent = 0)
      step = @root if step.nil?
      '  ' * indent + step.to_color + "\n" + step.children.map do |child_step|
        to_color child_step, indent + 1
      end.reduce('', &:+)
    end
    # :nocov:
  end

  # Thrown when it is not possible to construct a plan for a query
  class NoPlanException < StandardError
  end

  # A query planner which can construct a tree of query plans
  class QueryPlanner
    def initialize(model, indexes, cost_model)
      @logger = Logging.logger['nose::planner']

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
    # @return [QueryPlan]
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

require_relative 'filter'
require_relative 'index_lookup'
require_relative 'limit'
require_relative 'sort'
