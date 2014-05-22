require 'forwardable'

module Sadvisor
  # A single plan for a query
  class QueryPlan
    include Comparable
    include Enumerable

    # Most of the work is delegated to the array
    extend Forwardable
    def_delegators :@steps, :each, :<<, :[], :==, :===, :eql?,
                   :inspect, :to_a, :to_ary, :last

    def initialize
      @steps = []
    end

    # Two plans are compared by their execution cost
    def <=>(other)
      cost <=> other.cost
    end

    # The estimated cost of executing the query using this plan
    def cost
      @steps.map(&:cost).inject(0, &:+)
    end
  end

  # A single step in a query plan
  class PlanStep
    include Enumerable

    attr_accessor :children, :state
    attr_reader :fields

    def initialize
      @children = []
      @parent = nil
      @fields = Set.new
    end

    def inspect
      self.class.name.split(/(?=[A-Z])/)[0..-2] \
          .map(&:downcase).join(' ').capitalize
    end

    def children=(children)
      @children = children

      # Track the parent step of each step
      children.each do |child|
        child.instance_variable_set(:@parent, self)
        fields = child.instance_variable_get(:@fields) + self.fields
        child.instance_variable_set(:@fields, fields)
      end
    end

    def add_fields_from_index(index)
      @fields += index.fields + index.extra
    end

    # Get the list of steps which led us here as a QueryPlan
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

      return if state.nil?
      @state = state.dup
      update_state parent
    end

    def update_state(parent)
      # Find fields which are filtered by the index
      eq_filter = @state.eq & @index.fields
      range_filter = @index.fields.include?(state.range) ? state.range : nil

      @state.fields -= @index.fields + @index.extra
      @state.eq -= eq_filter
      @state.range = nil if @index.fields.include?(@state.range)
      @state.order_by -= @index.fields

      index_path = index.path
      cardinality = @state.cardinality
      if (index.path.first.id_fields.to_set - parent.fields - @state.eq).empty?
        index_path = index.path[1..-1] if index.path.length > 1
        cardinality = index.path[0].count if parent.is_a? RootStep
      end

      @state.cardinality = new_cardinality cardinality, eq_filter, range_filter

      last = state.path.last
      if index_path.length == 1
        @state.path = @state.path[1..-1]
      elsif state.path.length > 0
        @state.path = @state.path[index_path.length - 1..-1]
      end

      # If we still have fields left to fetch, we need to allow
      # another lookup
      if @state.path.length == 0 && @state.fields.count > 0
        @state.path = [last]
      end
    end

    def inspect
      "#{super} #{@index.inspect} * #{@state.cardinality}"
    end

    # Two index steps are equal if they use the same index
    def ==(other)
      other.instance_of?(self.class) && @index == other.index
    end

    # Rough cost estimate as the size of data returned
    def cost
      @state.cardinality * @index.entry_size
    end

    # Check if this step can be applied for the given index, returning an array
    # of possible applications of the step
    def self.apply(parent, index, state)
      # We have the fields that have already been looked up,
      # plus what was given in equality predidcates
      given_fields = parent.fields + state.eq

      # If we have the IDs for the first step, we can skip that in the index
      index_path = index.path
      if (index_path.first.id_fields.to_set - given_fields).empty?
        index_path = index.path[1..-1] if index_path.length > 1
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
      index_fields = (index.fields + index.extra).to_set

      if path_fields.all?(&index_fields.method(:include?)) &&
         (last_fields.all?(&index_fields.method(:include?)) ||
          index_path.last.id_fields.all?(&index_fields.method(:include?)))
        # TODO: Check that fields are usable for predicates
        return [IndexLookupStep.new(index, state, parent)]
      end

      []
    end

    def filter_cardinality(eq_filter, range_filter, entity)
      filter = range_filter && range_filter.entity == entity ? 0.1 : 1.0
      filter *= (eq_filter[entity] || []).map do |field|
        1.0 / field.cardinality
      end.inject(1.0, &:*)

      filter
    end

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

    def inspect
      super + ' ' + @sort_fields.map(&:inspect).to_s.gsub('"', '')
    end

    # Two sorting steps are equal if they sort on the same fields
    def ==(other)
      other.instance_of?(self.class) && @sort_fields == other.sort_fields
    end

    def cost
      # XXX Find some estimate of sort cost
      #     This could be partially captured by the fact that sort + limit
      #     effectively removes the limit
      0
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
    end

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

    # Two filtering steps are equal if they filter on the same fields
    def ==(other)
      other.instance_of?(self.class) && \
        @eq == other.eq && @range == other.range
    end

    def inspect
      "#{super} #{@eq.inspect} #{@range.inspect} " +
      begin
        "#{instance_variable_get(:@parent).state.cardinality} " \
        "-> #{state.cardinality}"
      rescue NoMethodError
        ''
      end
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

    # Check if filtering can be done (we have all the necessary fields)
    def self.apply(parent, state)
      # In case we try to filter at the first step in the chain
      # before fetching any data
      return nil if parent.is_a? RootStep

      # Get fields and check for possible filtering
      filter_fields, eq_filter, range_filter = self.filter_fields state
      return nil if filter_fields.empty?

      # Check that we have all the fields we are filtering
      given_fields = state.given_fields + parent.fields
      has_fields = filter_fields.map do |field|
        given_fields.member? field
      end.all?

      return FilterStep.new eq_filter, range_filter, state if has_fields

      nil
    end

    def cost
      # Assume this has no cost and the cost is captured in the fact that we
      # have to retrieve more data earlier. All this does is skip records.
      0
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
      @fields = query.fields.map { |field| workload.find_field field.value }
      @eq = query.eq_fields.map \
          { |condition| workload.find_field condition.field.value }.to_set
      @range = workload.find_field query.range_field.field.value \
          unless query.range_field.nil?
      @order_by = query.order_by.map { |field| workload.find_field field }
      @path = query.longest_entity_path.map(&workload.method(:[])).reverse
      @cardinality = @path.first.count
      @given_fields = @eq.dup

      check_first_path
    end

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

    def inspect
      @query.text_value +
          "\n  fields: " + @fields.map { |field| field.inspect }.to_a.to_s +
          "\n      eq: " + @eq.map { |field| field.inspect }.to_a.to_s +
          "\n   range: " + (@range.nil? ? '(nil)' : @range.name) +
          "\n   order: " + @order_by.map { |field| field.inspect }.to_a.to_s +
          "\n    path: " + @path.to_a.inspect
    end

    # Check if the query has been fully answered
    def answered?
      @fields.empty? && @eq.empty? && @range.nil? && @order_by.empty?
    end

    # Create a deep copy of the query state
    def dup
      # Ensure a deep copy
      Marshal.load(Marshal.dump(self))
    end
  end

  # A tree of possible query plans
  class QueryPlanTree
    include Enumerable

    attr_reader :root

    def initialize(state)
      @root = RootStep.new(state)
    end

    # Enumerate all steps in the given plan
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

    def size
      to_a.count
    end

    def inspect(step = nil, indent = 0)
      step = @root if step.nil?
      '  ' * indent + step.inspect + "\n" + step.children.map do |child_step|
        inspect child_step, indent + 1
      end.reduce('', &:+)
    end
  end

  # Thrown when it is not possible to construct a plan for a query
  class NoPlanException < StandardError
  end

  # A query planner which can construct a tree of query plans
  class Planner
    # rubocop:disable ClassVars

    # Possible steps which require indices
    @@index_steps = [
      IndexLookupStep
    ]

    # Possible steps which do not require indices
    @@index_free_steps = [
      SortStep,
      FilterStep
    ]

    # rubocop:enable all

    def initialize(workload, indexes)
      @workload = workload
      @indexes = indexes
    end

    # Find possible query plans for a query strating at the given step
    def find_plans_for_step(root, step)
      unless step.state.answered?
        steps = find_steps_for_state(step, step.state).select do |new_step|
          new_step != step
        end

        if steps.length > 0
          step.children = steps
          steps.each { |child_step| find_plans_for_step root, child_step }
        else fail NoPlanException
        end
      end
    end

    # Find a tree of plans for the given query
    def find_plans_for_query(query)
      state = QueryState.new query, @workload
      tree = QueryPlanTree.new(state)

      find_plans_for_step tree.root, tree.root

      tree
    end

    def min_query_cost(query)
      find_plans_for_query(query).min.cost
    end

    # Get a list of possible next steps for a query in the given state
    def find_steps_for_state(parent, state)
      steps = []

      @@index_free_steps.each \
          { |step| steps.push step.apply(parent, state) }
      steps.flatten!
      steps.compact!

      return steps if steps.length > 0
      @indexes.each do |index|
        @@index_steps.each do |step|
          steps.push step.apply(parent, index, state).each \
              { |new_step| new_step.add_fields_from_index index }
        end
      end
      steps.flatten.compact
    end
  end
end
