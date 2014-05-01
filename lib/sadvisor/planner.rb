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

      # We start with these fields because they were given in the query
      @fields |= state.eq
      @fields << state.range unless state.range.nil?
    end
  end

  # Superclass for steps using indices
  class IndexStep < PlanStep
    attr_reader :index

    def initialize(index)
      super()
      @index = index
    end

    def inspect
      super + ' ' + index.inspect
    end

    # Two index steps are equal if they use the same index
    def ==(other)
      other.instance_of?(self.class) && @index == other.index
    end

    def cost
      index.query_cost state.query, state.workload
    end
  end

  # A query plan step looking up fields based on IDs
  class IDLookupStep < IndexStep
    def self.apply(parent, index, state)
      entity = index.fields[0].parent
      return [] unless index.identity_for? entity

      new_fields = (Set.new(index.fields + index.extra) - parent.fields).length
      return [] unless new_fields > 0

      id_fields = parent.fields & Set.new(entity.id_fields)
      return [] unless id_fields.length > 0

      new_step = IDLookupStep.new(index)
      new_state = state.dup
      new_state.eq -= id_fields.to_a
      (index.fields + index.extra).each { |field| new_state.fields.delete field }
      new_step.state = new_state

      [new_step]
    end
  end

  # A query plan step performing an index lookup
  class IndexLookupStep < IndexStep
    # Check if the index is an identity map for a entity we need
    # XXX This doesn't currently work for entities other than the primary
    #     entity of the query
    def self.apply_identity_entity(parent, index, state, entity)
      return nil unless index.identity_for? entity

      new_fields = (Set.new(index.fields + index.extra) - parent.fields).length
      return nil unless new_fields > 0

      has_ids = (parent.fields & Set.new(entity.id_fields)).length > 0
      return nil if has_ids

      new_step = IndexLookupStep.new(index)
      new_state = state.dup
      (index.fields + index.extra).each { |field| new_state.fields.delete field }
      new_step.state = new_state

      new_step
    end

    # Check if we can look up using an identity map for some entity
    def self.apply_identity(parent, index, state)
      state.entities.map do |entity, key_fields|
        # If the entity is referenced by one field, it is the key, so we can
        # proceed, otherwise we must have all the necessary keys
        next unless key_fields.length == 1 || \
                    key_fields.map do |fields|
                      fields.to_set.subset? parent.fields.to_set
                    end.all?

        apply_identity_entity(parent, index, state, entity)
      end.compact
    end

    def self.apply_filter(parent, index, state)
      # Try all possible combinations of equality predicates and ordering
      field_combos = (state.eq.prefixes.to_a << [])\
             .product(state.order_by.prefixes.to_a << [])
      field_combos = field_combos.select do |eq, order|
        # TODO: Check that keys are the same
        eq + order == index.fields
      end
      max_eq, max_order = field_combos.max_by \
          { |eq, order| eq.length + order.length } || [[], []]

      # TODO: Add range predicates

      if (max_eq.length > 0 || max_order.length > 0) &&
          # Either we have no fields
          (parent.fields.length == 0 ||
           # Or we're doing a lookup on the set of fields we know from predicates
           (parent.fields == max_eq.to_set && parent.instance_of?(RootStep)))

        # Ensure we actually get new fields we need
        new_fields = (index.fields + index.extra).to_set - parent.fields
        required_fields = state.entities.values.flatten.to_set + \
                          state.order_by.to_set + state.fields.to_set + \
                          state.entities.keys.map(&:id_fields).flatten.to_set

        return nil unless (new_fields & required_fields).length > 0

        new_step = IndexLookupStep.new(index)
        new_state = state.dup

        new_state.eq = new_state.eq[max_eq.length..-1] \
            if max_eq.length > 0
        new_state.order_by = new_state.order_by[max_order.length..-1] \
            if max_order.length > 0
        (index.fields + index.extra).each \
            { |field| new_state.fields.delete field }

        new_step.state = new_state

        [new_step]
      end
    end

    # Check if this step can be applied for the given index, returning an array
    # of possible applications of the step
    def self.apply(parent, index, state)
      all_new_steps = []
      [:apply_filter, :apply_identity].each do |strategy|
        new_steps = send(strategy, parent, index, state)
        all_new_steps += new_steps unless new_steps.nil? || new_steps.empty?
      end

      all_new_steps
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
      super + ' ' + @sort_fields.map { |field| field.inspect }.to_s.gsub('"', '')
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
    def self.apply(parent, state)
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

    def initialize(eq, range)
      @eq = eq
      @range = range
      super()
    end

    # Two filtering steps are equal if they filter on the same fields
    def ==(other)
      other.instance_of?(self.class) && @eq == other.eq && @range == other.range
    end

    def inspect
      super + ' ' + @eq.inspect + ' ' + @range.inspect
    end

    # Check if the plan has all the necessary fields to filter
    def self.contains_all_fields?(parent, state, range_field)
      filter_fields = state.eq.dup
      filter_fields << range_field unless range_field.nil?
      filter_fields.map { |field| parent.fields.member? field }.all?
    end

    # Check if filtering can be done (we have all the necessary fields)
    def self.apply(parent, state)
      if state.fields.empty? && !(state.eq.empty? && state.range.nil?) &&
          contains_all_fields?(parent, state, state.range)
        new_step = FilterStep.new(state.eq, state.range)

        new_state = state.dup
        new_state.eq = []
        new_state.range = nil
        new_step.state = new_state

        return new_step
      end

      nil
    end

    def cost
      # Assume this has no cost and the cost is captured in the fact that we have
      # to retrieve more data earlier. All this does is skip records.
      0
    end
  end

  # Ongoing state of a query throughout the execution plan
  class QueryState
    attr_accessor :from, :fields, :eq, :range, :order_by
    attr_reader :query, :entities, :workload

    def initialize(query, workload)
      @query = query
      @workload = workload
      @from = workload[query.from.value]
      @fields = query.fields.map { |field| workload.find_field field.value }
      @eq = query.eq_fields.map \
          { |condition| workload.find_field condition.field.value }
      @range = workload.find_field query.range_field.field.value \
          unless query.range_field.nil?
      @order_by = query.order_by.map { |field| workload.find_field field }

      populate_entities
    end

    # Track all entities accessed in this query
    def populate_entities
      @entities = { @from => [@from.id_fields] }
      fields = @query.order_by + @query.eq_fields.map do |condition|
        condition.field.value
      end
      fields << @query.range_field.field.value unless @query.range_field.nil?
      fields.each do |field|
        entity = @workload.find_field(field).parent
        @entities[entity] = @workload.find_field_keys field
      end

      expand_entities
    end

    # Expand the list of entities to those tracked by foreign keys
    def expand_entities
      new_entities = {}
      @entities.each do |entity, keys|
        keys.prefixes.each do |key_fields|
          next if key_fields.empty?
          new_entities[key_fields.last.first.parent] = key_fields[0..-2]
        end
      end
      @entities.merge! new_entities
    end

    def inspect
      @query.text_value +
          "\n  fields: " + @fields.map { |field| field.inspect }.to_a.to_s +
          "\n      eq: " + @eq.map { |field| field.inspect }.to_a.to_s +
          "\n   range: " + (@range.nil? ? '(nil)' : @range.name) +
          "\n   order: " + @order_by.map { |field| field.inspect }.to_a.to_s
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
      IDLookupStep,
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

      @indexes.each do |index|
        @@index_steps.each do |step|
          steps.push step.apply(parent, index, state).each \
              { |new_step| new_step.add_fields_from_index index }
        end
      end

      # Some steps may be applicable in multiple ways
      steps.flatten.compact
    end
  end
end
