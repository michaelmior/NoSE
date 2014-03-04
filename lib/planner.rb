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

class Planner
  def initialize(workload, indexes)
    @workload = workload
    @indexes = indexes
  end

  def find_plan_for_query(query, workload)
    for index in @indexes
      if index.supports_query? query, workload
        return [IndexLookupStep.new(index)]
      end
    end
  end
end
