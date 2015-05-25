module NoSE::Plans
  # A simple state class to hold the cardinality for updates
  class UpdateState
    attr_reader :statement, :cardinality

    def initialize(statement, cardinality)
      @statement = statement
      @cardinality = cardinality
    end

    # XXX This is just a placeholder since this is
    #     currently used by the query planner
    def answered?
      true
    end
  end

  # A planner for update statements in the workload
  class UpdatePlanner
    def initialize(model, indexes, cost_model)
      @logger = Logging.logger['nose::update_planner']

      @model = model
      @indexes = indexes
      @cost_model = cost_model
      @query_planner = QueryPlanner.new model, indexes, cost_model
    end

    # Find the necessary update plans for a given set of indexes
    def find_plans_for_update(statement, indexes)
      indexes.map do |index|
        queries = statement.support_queries(index)
        next [] if queries.empty?

        queries.map do |query|
          tree = @query_planner.find_plans_for_query query

          # Walk the tree and add delete and insert steps at the end
          tree.each do |plan|
            last_step = plan.last
            state = UpdateState.new statement, last_step.state.cardinality
            if statement.requires_delete?
              last_step.children = [DeletePlanStep.new(index, state)]
              last_step = last_step.children.first
            end
            if statement.requires_insert?
              last_step.children = [InsertPlanStep.new(index, state)]
            end
          end

          tree
        end
      end.flatten(1)
    end
  end
end
