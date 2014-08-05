require 'forwardable'

module Sadvisor
  # A single plan for a query
  class QueryPlan
    include Comparable
    include Enumerable

    # Most of the work is delegated to the array
    extend Forwardable
    def_delegators :@steps, :each, :<<, :[], :==, :===, :eql?,
                   :inspect, :to_s, :to_a, :to_ary, :last

    def initialize
      @steps = []
    end

    # Two plans are compared by their execution cost
    def <=>(other)
      cost <=> other.cost
    end

    # The estimated cost of executing the query using this plan
    # @return [Numeric]
    def cost
      @steps.map(&:cost).inject(0, &:+)
    end
  end

  # A single step in a query plan
  class PlanStep
    attr_accessor :children, :state
    attr_reader :fields

    def initialize
      @children = []
      @parent = nil
      @fields = Set.new
    end

    # :nocov:
    def to_color
      self.class.name.split('::').last.split(/(?=[A-Z])/)[0..-2] \
          .map(&:downcase).join(' ').capitalize
    end
    # :nocov:

    def children=(children)
      @children = children

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
    # @return [QueryPlan]
    def parent_steps
      steps = nil

      if @parent.nil?
        steps = QueryPlan.new
      else
        steps = @parent.parent_steps
        steps << self
      end

      steps
    end

    # The cost of executing this step in the plan
    # @return [Numeric]
    def cost
      0
    end
  end

  # The root of a tree of query plans used as a placeholder
  class RootStep < PlanStep
    def initialize(state)
      super()
      @state = state
    end
  end

  # Superclass for steps using indices
  class IndexLookupStep < PlanStep
    attr_reader :index

    def initialize(index, state = nil, parent = nil)
      super()
      @index = index

      if state && state.query
        all_fields = state.query.all_fields.map do |field|
          state.workload.find_field field
        end
        @fields = (@index.hash_fields + @index.order_fields).to_set + \
          (@index.extra.to_set & all_fields)
      else
        @fields = @index.all_fields
      end

      return if state.nil?
      @state = state.dup
      update_state parent
      @state.freeze
    end

    # :nocov:
    def to_color
      "#{super} #{@index.to_color} * #{@state.cardinality} " + \
        "$#{cost}".yellow
    end
    # :nocov:

    # Two index steps are equal if they use the same index
    def ==(other)
      other.instance_of?(self.class) && @index == other.index
    end

    # Rough cost estimate as the size of data returned
    # @return [Numeric]
    def cost
      @state.cardinality * @fields.map(&:size).inject(0, :+)
    end

    # Check if this step can be applied for the given index, returning an array
    # of possible applications of the step
    def self.apply(parent, index, state)
      # Check for the case where only a simple lookup is needed
      if state.path.length == 0 && state.fields.count > 0
        if index == state.fields.first.parent.simple_index
          return [IndexLookupStep.new(index, state, parent)]
        else
          return []
        end
      end

      # We have the fields that have already been looked up,
      # plus what was given in equality predidcates
      given_fields = parent.fields + state.eq

      # If we have the IDs for the first step, we can skip that in the index
      index_path = index.path
      if (index_path.first.id_fields.to_set - given_fields).empty? &&
          index_path.length > 1 && state.path.length > 1
        index_path = index.path[1..-1]
      end

      # Check that this index is a valid jump in the path
      return [] unless state.path[0..index_path.length - 1] == index_path

      # Check that all required fields are included in the index
      path_fields = state.eq + state.order_by
      path_fields << state.range unless state.range.nil?

      last_fields = path_fields.select do |field|
        field.parent == index_path.last
      end
      path_fields = path_fields.select do |field|
        next if field.parent == index_path.last
        index_path.include? field.parent
      end

      # Make sure we have the final required fields in the index
      if path_fields.all?(&index.all_fields.method(:include?)) &&
         (last_fields.all?(&index.all_fields.method(:include?)) ||
          index_path.last.id_fields.all?(&index.all_fields.method(:include?)))
        # TODO: Check that fields are usable for predicates
        return [IndexLookupStep.new(index, state, parent)]
      end

      []
    end

    private

    # Modify the state to reflect the fields looked up by the index
    def update_state(parent)
      # Find fields which are filtered by the index
      eq_filter = @state.eq & (@index.hash_fields + @index.order_fields)
      range_filter = @index.order_fields.include?(state.range) ? \
        state.range : nil

      @state.fields -= @index.all_fields
      @state.eq -= eq_filter
      @state.range = nil if @index.order_fields.include?(@state.range)
      @state.order_by -= @index.order_fields

      index_path = index.path
      cardinality = @state.cardinality
      if (index.path.first.id_fields.to_set - parent.fields - @state.eq).empty?
        index_path = index.path[1..-1] if index.path.length > 1
        cardinality = index.path[0].count if parent.is_a? RootStep
      end

      @state.cardinality = new_cardinality cardinality, eq_filter, range_filter

      if index_path.length == 1
        @state.path = @state.path[1..-1]
      elsif state.path.length > 0
        @state.path = @state.path[index_path.length - 1..-1]
      end
      @state.path = [] if @state.path.nil?
    end

    # Update the cardinality based on filtering implicit to the index
    def filter_cardinality(eq_filter, range_filter, entity)
      filter = range_filter && range_filter.parent == entity ? 0.1 : 1.0
      filter *= (eq_filter[entity] || []).map do |field|
        1.0 / field.cardinality
      end.inject(1.0, &:*)

      filter
    end

    # Update the cardinality after traversing the index
    def new_cardinality(cardinality, eq_filter, range_filter)
      eq_filter = eq_filter.group_by(&:parent)
      index_path = @index.path.reverse

      # Update cardinality via predicates for first (last) entity in path
      cardinality *= filter_cardinality eq_filter, range_filter,
                                        index_path.first

      index_path.each_cons(2) do |entity, next_entity|
        tail = entity.foreign_key_for(next_entity).nil?
        if tail
          cardinality = cardinality * 1.0 * next_entity.count / entity.count
        else
          cardinality = sample 1.0 * cardinality, next_entity.count
        end

        # Update cardinality via the filtering implicit to the index
        cardinality *= filter_cardinality eq_filter, range_filter, next_entity
      end

      cardinality.ceil
    end

    # Get the estimated cardinality of the set of samples of m items with
    # replacement from a set of cardinality n
    def sample(m, n)
      samples = [1.0]

      1.upto(m - 1).each do
        samples << ((n - 1) * samples[-1] + n) / n
      end

      samples[-1]
    end
  end

  # A query plan step performing external sort
  class SortStep < PlanStep
    attr_reader :sort_fields

    def initialize(sort_fields)
      super()
      @sort_fields = sort_fields
    end

    # :nocov:
    def to_color
      super + ' [' + @sort_fields.map(&:to_color).join(', ') + ']'
    end
    # :nocov:

    # Two sorting steps are equal if they sort on the same fields
    def ==(other)
      other.instance_of?(self.class) && @sort_fields == other.sort_fields
    end

    # (see PlanStep#cost)
    def cost
      # TODO: Find some estimate of sort cost
      #       This could be partially captured by the fact that sort + limit
      #       effectively removes the limit
      1
    end

    # Check if an external sort can used (if a sort is the last step)
    def self.apply(_parent, state)
      new_step = nil

      if state.fields.empty? && state.eq.empty? && state.range.nil? && \
        !state.order_by.empty?

        new_state = state.dup
        new_state.order_by = []
        new_step = SortStep.new(state.order_by)
        new_step.state = new_state
        new_step.state.freeze
      end

      new_step
    end
  end

  # A query plan performing a filter without an index
  class FilterStep < PlanStep
    attr_reader :eq, :range

    def initialize(eq, range, state = nil)
      @eq = eq
      @range = range
      super()

      return if state.nil?
      @state = state.dup
      update_state
      @state.freeze
    end

    # Two filtering steps are equal if they filter on the same fields
    def ==(other)
      other.instance_of?(self.class) && \
        @eq == other.eq && @range == other.range
    end

    # :nocov:
    def to_color
      "#{super} #{@eq.to_color} #{@range.to_color} " +
      begin
        "#{instance_variable_get(:@parent).state.cardinality} " \
        "-> #{state.cardinality}"
      rescue NoMethodError
        ''
      end
    end
    # :nocov:

    # (see PlanStep#cost)
    def cost
      # Assume this has no cost and the cost is captured in the fact that we
      # have to retrieve more data earlier. All this does is skip records.
      0
    end

    # Check if filtering can be done (we have all the necessary fields)
    def self.apply(parent, state)
      # In case we try to filter at the first step in the chain
      # before fetching any data
      return nil if parent.is_a? RootStep

      # Get fields and check for possible filtering
      filter_fields, eq_filter, range_filter = filter_fields state
      return nil if filter_fields.empty?

      # Check that we have all the fields we are filtering
      has_fields = filter_fields.map do |field|
        next true if parent.fields.member? field

        # We can also filter if we have a foreign key
        # XXX for now we assume this value is the same
        if field.is_a? IDField
          parent.fields.any? do |pfield|
            pfield.is_a?(ForeignKey) && pfield.entity == field.parent
          end
        end
      end.all?

      return FilterStep.new eq_filter, range_filter, state if has_fields

      nil
    end

    # Get the fields we can possibly filter on
    def self.filter_fields(state)
      eq_filter = state.eq.select { |field| !state.path.member? field.parent }
      filter_fields = eq_filter.dup
      if state.range && !state.path.member?(state.range.parent)
        range_filter = state.range
        filter_fields << range_filter
      else
        range_filter = nil
      end

      [filter_fields, eq_filter, range_filter]
    end
    private_class_method :filter_fields

    private

    # Apply the filters and perform a uniform estimate on the cardinality
    def update_state
      @state.eq -= @eq
      @state.cardinality *= @eq.map { |field| 1.0 / field.cardinality } \
        .inject(1.0, &:*)

      if @range
        @state.range = nil
        @state.cardinality *= 0.1
      end
      @state.cardinality = @state.cardinality.ceil
    end
  end

  # Ongoing state of a query throughout the execution plan
  class QueryState
    attr_accessor :from, :fields, :eq, :range, :order_by, :path, :cardinality,
                  :given_fields
    attr_reader :query, :entities, :workload

    def initialize(query, workload)
      @query = query
      @workload = workload
      @from = workload[query.from.value]
      @fields = query.fields.map do |field|
        workload.find_field field.value
      end.to_set
      @eq = query.eq_fields.map do |condition|
        workload.find_field condition.field.value
      end.to_set
      @range = workload.find_field query.range_field.field.value \
          unless query.range_field.nil?
      @order_by = query.order_by.map { |field| workload.find_field field }
      @path = query.longest_entity_path.map(&workload.method(:[])).reverse
      @cardinality = @path.first.count
      @given_fields = @eq.dup

      check_first_path
    end

    # All the fields referenced anywhere in the query
    def all_fields
      all_fields = @fields + @eq
      all_fields << @range unless @range.nil?
      all_fields
    end

    # :nocov:
    def to_color
      @query.text_value +
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
    def answered?
      @fields.empty? && @eq.empty? && @range.nil? && @order_by.empty?
    end

    private

    # Remove the first element from the path if we only have the ID
    def check_first_path
      first_fields = @eq + (@range ? [@range] : [])
      first_fields = first_fields.select do |field|
        field.parent == @path.first
      end
      if first_fields == @path.first.id_fields && @path.length > 1
        @path.shift
      end
    end
  end

  # A tree of possible query plans
  class QueryPlanTree
    include Enumerable

    attr_reader :root

    def initialize(state)
      @root = RootStep.new(state)
    end

    # Enumerate all plans in the tree
    def each
      nodes = [@root]

      while nodes.length > 0
        node = nodes.pop
        if node.children.length > 0
          nodes.concat node.children
        else
          yield node.parent_steps
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
  class Planner
    def initialize(workload, indexes)
      @workload = workload
      @indexes = indexes
    end

    # Find a tree of plans for the given query
    # @return [QueryPlanTree]
    # @raise [NoPlanException]
    def find_plans_for_query(query)
      state = QueryState.new query, @workload
      state.freeze
      tree = QueryPlanTree.new(state)

      find_plans_for_step tree.root, tree.root

      tree
    end

    # Get the minimum cost plan for executing this query
    # @return [QueryPlan]
    def min_plan(query)
      find_plans_for_query(query).min
    end

    private

    # Find possible query plans for a query strating at the given step
    # @raise [NoPlanException]
    def find_plans_for_step(root, step)
      unless step.state.answered?
        steps = find_steps_for_state(step, step.state)

        if steps.length > 0
          step.children = steps
          steps.each { |child_step| find_plans_for_step root, child_step }
        elsif step == root
          fail NoPlanException
        else
          # Walk up the tree and remove the branch for the failed plan
          prune_step = step.instance_variable_get(:@parent)
          prev_step = step
          while prune_step.children.length <= 1 && prune_step != root
            prune_step = prune_step.instance_variable_get(:@parent)
            prev_step = prune_step
          end

          # If we reached the root, we have no plan
          fail NoPlanException if prune_step == root

          prune_step.children.delete prev_step
        end
      end
    end

    # Get a list of possible next steps for a query in the given state
    # @return [Array<PlanStep>]
    # @raise [NoPlanException]
    def find_steps_for_state(parent, state)
      steps = []

      [SortStep, FilterStep].each \
        { |step| steps.push step.apply(parent, state) }
      steps.flatten!
      steps.compact!

      return steps if steps.length > 0

      # Don't allow indices to be used multiple times
      used_indexes = parent.parent_steps.select do |step|
        step.kind_of? IndexLookupStep
      end.map(&:index)
      (@indexes - used_indexes).each do |index|
        steps.push IndexLookupStep.apply(parent, index, state).each \
            { |new_step| new_step.add_fields_from_index index }
      end
      steps.flatten.compact
    end
  end
end
