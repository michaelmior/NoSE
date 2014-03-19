require 'forwardable'

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

  def <=>(other)
    cost <=> other.cost
  end

  def cost
    @steps.map(&:cost).inject(0, &:+)
  end
end

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
    children.each { |child| child.instance_variable_set(:@parent, self) }
  end

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

  def cost
    0
  end
end

class RootStep < PlanStep
  def initialize(state)
    super()
    @state = state
  end
end

class IndexLookupStep < PlanStep
  attr_reader :index

  def initialize(index)
    super()
    @index = index
  end

  def inspect
    super + ' ' + index.inspect
  end

  def ==(other)
    other.instance_of?(self.class) && @index == other.index
  end

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

class SortStep < PlanStep
  attr_reader :fields

  def initialize(fields)
    super()
    @fields = fields
  end

  def inspect
    super + ' ' + @fields.map { |field| field.inspect }.to_s.gsub('"', '')
  end

  def ==(other)
    other.instance_of?(self.class) && @fields == other.fields
  end

  def cost
    # XXX Find some estimate of sort cost
    #     This could be partially captured by the fact that sort + limit
    #     effectively removes the limit
    0
  end

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

  def answered?
    @fields.empty? && @eq.empty? && @range.nil? && @order_by.empty?
  end

  def dup
    Marshal.load(Marshal.dump(self))
  end
end

class QueryPlanTree
  include Enumerable

  attr_reader :root

  def initialize(state)
    @root = RootStep.new(state)
  end

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
    "  " * indent + step.inspect + "\n" + step.children.map do |child_step|
      inspect child_step, indent + 1
    end.reduce('', &:+)
  end
end

class NoPlanException < StandardError
end

class Planner
  @@index_steps = [
    IndexLookupStep
  ]

  @@index_free_steps = [
    SortStep
  ]

  def initialize(workload, indexes)
    @workload = workload
    @indexes = indexes
  end

  def find_plan_for_query(query, workload)
    find_plans_for_query(query, workload)
  end

  def find_plans_for_step(step, workload)
    unless step.state.answered?
      steps = find_steps_for_state(step.state, workload).select do |new_step|
        new_step != step
      end

      if steps.length > 0
        step.children = steps
        steps.each { |child_step| find_plans_for_step child_step, workload }
      else
        fail NoPlanException
      end
    end
  end

  def find_plans_for_query(query, workload)
    state = QueryState.new query
    plan = QueryPlanTree.new(state)

    find_plans_for_step(plan.root, workload)

    plan
  end

  def find_steps_for_state(state, workload)
    steps = []

    @@index_free_steps.each do |step|
      new_step = step.apply(state, workload)
      steps.push new_step if new_step
    end

    @indexes.each do |index|
      @@index_steps.each do |step|
        new_step = step.apply(index, state, workload)
        steps.push new_step if new_step
      end
    end

    steps.flatten
  end
end
