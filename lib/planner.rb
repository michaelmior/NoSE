class PlanStep
end

class IndexLookupStep < PlanStep
  attr_reader :index

  def initialize(index)
    @index = index
  end

  def ==(other)
    @index == other.index
  end
end

class NoPlanException < StandardError
end

class Planner
  def initialize(workload, indexes)
    @workload = workload
    @indexes = indexes
  end

  def find_plan_for_query(query, workload)
    steps = []

    from = query.from.value
    fields = query.fields
    eq = query.eq_fields
    range = [query.range_field]
    order_by = query.order_by

    until fields.empty? and eq.empty? and range.empty? and order_by.empty?
      step = find_step_for_predicates(from, fields, eq, range, order_by, workload)
      if step
        steps.push step
      else
        raise NoPlanException
      end
    end

    steps
  end

  def find_step_for_predicates(from, fields, eq, range, order_by, workload)
    next_steps = []

    for index in @indexes
      if index.supports_predicates?(from, fields, eq, range[0], order_by, workload)
        [fields, eq, range, order_by].each { |ary| ary.collect!{ nil }.compact! }
        next_steps.push IndexLookupStep.new(index)
        break
      end
    end

    next_steps.sort!.first
  end
end
