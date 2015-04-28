module NoSE::Plans
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
        query = statement.support_query(index)
        next if query.nil?
        tree = @query_planner.find_plans_for_query query

        # Walk the tree and add delete and insert steps at the end
        tree.each do |plan|
          last_step = plan.last
          unless statement.is_a? NoSE::Insert
            last_step.children = [DeletePlanStep.new(index, last_step.state)]
            last_step = last_step.children.first
          end
          unless statement.is_a? NoSE::Delete
            last_step.children = [InsertPlanStep.new(index, last_step.state)]
          end
        end

        tree
      end
    end
  end
end
