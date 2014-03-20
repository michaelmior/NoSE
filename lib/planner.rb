require 'forwardable'

# Asingle plan for a query
class QueryPlan
  include Comparable
  include Enumerable

  # Most of the work is delegated to the array
  extend Forwardable
  def_delegators :@steps, :each, :<<, :[], :==, :===, :eql?,
                 :inspect, :to_a, :to_ary

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

  attr_accessor :children
  attr_accessor :state

  def initialize
    @children = []
    @parent = nil
  end

  def inspect
    self.class.name.split(/(?=[A-Z])/)[0..-2] \
        .map(&:downcase).join(' ').capitalize
  end

  def children=(children)
    @children = children

    # Track the parent step of each step
    children.each { |child| child.instance_variable_set(:@parent, self) }
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

# A query plan step performing an index lookup
class IndexLookupStep < PlanStep
  attr_reader :index

  def initialize(index)
    super()
    @index = index
  end

  def inspect
    super + ' ' + index.inspect
  end

  # Two index lookups are equal if they use the same index
  def ==(other)
    other.instance_of?(self.class) && @index == other.index
  end

  # Check if this step can be applied for the given index, returning an array
  # of possible applications of the step
  def self.apply(index, state, workload)
    new_steps = []

    if index.supports_predicates?(state.from, state.fields, state.eq, \
                                  state.range, state.order_by, workload)

      new_state = state.dup
      new_state.fields = []
      new_state.eq = []
      new_state.range = nil
      new_state.order_by = []
      new_step = IndexLookupStep.new(index)
      new_step.state = new_state
      new_steps.push new_step
    end

    return new_steps if (state.fields.empty? && \
        state.eq.empty? && state.range.nil?) || state.order_by.length == 0

    # Can do everything but ordering (results in an external sort later)
    if index.supports_predicates?(state.from, state.fields, state.eq, \
                                  state.range, [], workload)

      new_state = state.dup
      new_state.fields = []
      new_state.eq = []
      new_state.range = nil
      new_step = IndexLookupStep.new(index)
      new_step.state = new_state

      new_steps.push new_step
    end

    new_steps.length > 0 ? new_steps : nil
  end
end

# A query plan step performing external sort
class SortStep < PlanStep
  attr_reader :fields

  def initialize(fields)
    super()
    @fields = fields
  end

  def inspect
    super + ' ' + @fields.map { |field| field.inspect }.to_s.gsub('"', '')
  end

  # Two sorting steps are equal if they sort on the same fields
  def ==(other)
    other.instance_of?(self.class) && @fields == other.fields
  end

  def cost
    # XXX Find some estimate of sort cost
    #     This could be partially captured by the fact that sort + limit
    #     effectively removes the limit
    0
  end

  # Check if an external sort can used (if a sort is the last step)
  def self.apply(state, workload)
    new_step = nil

    if state.fields.empty? && state.eq.empty? && state.range.nil? && \
      !state.order_by.empty?
      order_fields = state.order_by.map { |field| workload.find_field field }

      state = state.dup
      state.order_by = []
      new_step = SortStep.new(order_fields)
      new_step.state = state
    end

    new_step
  end
end

# Ongoing state of a query throughout the execution plan
class QueryState
  attr_accessor :from
  attr_accessor :fields
  attr_accessor :eq
  attr_accessor :range
  attr_accessor :order_by

  def initialize(query)
    @query = query
    @from = query.from.value
    @fields = query.fields
    @eq = query.eq_fields
    @range = query.range_field
    @order_by = query.order_by
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
    IndexLookupStep
  ]

  # Possible steps which do not require indices
  @@index_free_steps = [
    SortStep
  ]

  # rubocop:enable all

  def initialize(workload, indexes)
    @workload = workload
    @indexes = indexes
  end

  # Find possible query plans for a query strating at the given step
  def find_plans_for_step(step)
    unless step.state.answered?
      steps = find_steps_for_state(step.state).select do |new_step|
        new_step != step
      end

      if steps.length > 0
        step.children = steps
        steps.each { |child_step| find_plans_for_step child_step }
      else fail NoPlanException
      end
    end
  end

  # Find a tree of plans for the given query
  def find_plans_for_query(query)
    state = QueryState.new query
    tree = QueryPlanTree.new(state)

    find_plans_for_step tree.root

    tree
  end

  # Get a list of possible next steps for a query in the given state
  def find_steps_for_state(state)
    steps = []

    @@index_free_steps.each do |step|
      steps.push step.apply(state, @workload)
    end

    @indexes.each do |index|
      @@index_steps.each do |step|
        steps.push step.apply(index, state, @workload)
      end
    end

    # Some steps may be applicable in multiple ways
    steps.flatten.compact
  end
end
