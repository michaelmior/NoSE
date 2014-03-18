class PlanStep
  attr_accessor :children
  attr_accessor :state

  def initialize
    @children = []
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

  def ==(other)
    other.instance_of?(self.class) && @index == other.index
  end

  def self.apply(index, state, workload)
    if index.supports_predicates?(state.from, state.fields, state.eq, \
                                  state.range, state.order_by, workload)

      state = state.dup
      state.fields = []
      state.eq = []
      state.range = nil
      state.order_by = []
      new_step = IndexLookupStep.new(index)
      new_step.state = state
      return new_step
    end

    return nil if state.fields.empty? && state.eq.empty? && state.range.nil?

    if index.supports_predicates?(state.from, state.fields, state.eq, \
                                  state.range, [], workload)

      state = state.dup
      state.fields = []
      state.eq = []
      state.range = nil
      new_step = IndexLookupStep.new(index)
      new_step.state = state

      return new_step
    end

    nil
  end
end

class SortStep < PlanStep
  attr_reader :fields

  def initialize(fields)
    super()
    @fields = fields
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
    @from = query.from.value
    @fields = query.fields
    @eq = query.eq_fields
    @range = query.range_field
    @order_by = query.order_by
  end

  def answered?
    @fields.empty? && @eq.empty? && @range.nil? && @order_by.empty?
  end
end

class QueryPlan
  attr_reader :root

  def initialize(state)
    @root = RootStep.new(state)
  end

  def first
    plan = []
    step = @root
    while step.children.length > 0
      step = step.children[0]
      plan.push step
    end

    plan
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
      steps = find_steps_for_state(step.state, workload)
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
    plan = QueryPlan.new(state)

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

    steps
  end
end
