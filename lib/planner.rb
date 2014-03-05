class PlanStep
end

class IndexLookupStep < PlanStep
  attr_reader :index

  def initialize(index)
    @index = index
  end

  def ==(other)
    other.instance_of? self.class and @index == other.index
  end
end

class SortStep < PlanStep
  attr_reader :fields

  def initialize(fields)
    @fields = fields
  end

  def ==(other)
    other.instance_of? self.class and @fields == other.fields
  end

  def cost
    # XXX Find some estimate of sort cost
    #     This could be partially captured by the fact that sort + limit
    #     effectively removes the limit
    0
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
    range = query.range_field ? [query.range_field] : []
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
    for index in @indexes
      if fields.empty? and eq.empty? and range.empty? and not order_by.empty?
        order_fields = order_by.map { |field| workload.find_field field }
        order_by.collect!{ nil }.compact!
        return SortStep.new(order_fields)
      end

      if index.supports_predicates?(from, fields, eq, range[0], order_by, workload)
        [fields, eq, range, order_by].each { |ary| ary.collect!{ nil }.compact! }
        return IndexLookupStep.new(index)
      end

      if index.supports_predicates?(from, fields, eq, range[0], [], workload)
        [fields, eq, range].each { |ary| ary.collect!{ nil }.compact! }
        return IndexLookupStep.new(index)
      end
    end

    nil
  end
end
