class PlanStep
end

class IndexLookupStep < PlanStep
  attr_reader :index

  def initialize(index)
    @index = index
  end

  def ==(other)
    other.instance_of?(self.class) && @index == other.index
  end

  def self.apply(index, state, workload)
    if index.supports_predicates?(state.from, state.fields, state.eq, \
                                  state.range, state.order_by, workload)
      state.fields = []
      state.eq = []
      state.range = nil
      state.order_by = []
      return IndexLookupStep.new(index)
    end

    if index.supports_predicates?(state.from, state.fields, state.eq, \
                                  state.range, [], workload)
      state.fields = []
      state.eq = []
      state.range = nil
      return IndexLookupStep.new(index)
    end

    nil
  end
end

class SortStep < PlanStep
  attr_reader :fields

  def initialize(fields)
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
    if state.fields.empty? && state.eq.empty? && state.range.nil? && \
      !state.order_by.empty?
      order_fields = state.order_by.map { |field| workload.find_field field }
      state.order_by = []
      return SortStep.new(order_fields)
    end

    nil
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
    steps = []
    state = QueryState.new query

    until state.answered?
      step = find_step_for_state(state, workload)
      if step
        steps.push step
      else
        fail NoPlanException
      end
    end

    steps
  end

  def find_step_for_state(state, workload)
    @@index_free_steps.each do |step|
      new_step = step.apply(state, workload)
      return new_step if new_step
    end

    @indexes.each do |index|
      @@index_steps.each do |step|
        new_step = step.apply(index, state, workload)
        return new_step if new_step
      end
    end

    nil
  end
end
