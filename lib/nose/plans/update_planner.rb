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

  # A plan for executing an update
  class UpdatePlan
    attr_accessor :statement, :index, :query_plans, :update_steps, :cost_model

    include Comparable

    def initialize(statement, index, query_plans, update_steps, cost_model)
      @statement = statement
      @index = index
      @query_plans = query_plans
      @update_steps = update_steps
      @cost_model = cost_model
    end

    # Compare all the fields for the plan for equality
    def eql?(other)
      return false unless other.is_a? UpdatePlan

      @statement == other.statement &&
        @index == other.index &&
        @query_plans == other.query_plans &&
        @update_steps == other.update_steps && @cost_model == other.cost_model
    end

    # :nocov:
    def to_color
      "\n   statement: " + @statement.to_color +
        "\n       index: " + @index.to_color +
        "\n query_plans: " + @query_plans.to_color +
        "\nupdate_steps: " + @update_steps.to_color +
        "\n  cost_model: " + @cost_model.to_color
    end
    # :nocov:

    # Two plans are compared by their execution cost
    def <=>(other)
      cost <=> other.cost
    end

    # The cost is the sum of all the query costs plus the update costs
    def cost
      query_costs = @query_plans.map(&:cost)
      update_costs = update_steps.map { |step| step.cost @cost_model }

      (query_costs + update_costs).inject(0, &:+)
    end
  end

  # A planner for update statements in the workload
  class UpdatePlanner
    def initialize(model, query_plans, cost_model)
      @logger = Logging.logger['nose::update_planner']

      @model = model
      @cost_model = cost_model

      # query_plans is a nested dictionary keyed by statement and then index
      @query_plans = query_plans
    end

    # Find the necessary update plans for a given set of indexes
    def find_plans_for_update(statement, indexes)
      indexes.map do |index|
        next unless statement.modifies_index?(index)

        unless (@query_plans[statement] && @query_plans[statement][index]).nil?
          # Get the cardinality of the last step to use for the update state
          plans = @query_plans[statement][index].map do |tree|
            tree.select_using_indexes(indexes).min_by(&:cost)
          end
          last_step = plans.first.last
          state = UpdateState.new statement, last_step.state.cardinality
        else
          plans = []
          cardinality = Cardinality.new_cardinality statement.from.count,
                                                    statement.eq_fields,
                                                    statement.range_field,
                                                    statement.longest_entity_path
          state = UpdateState.new statement, cardinality
        end

        # Find the required update steps
        update_steps = []
        update_steps << DeletePlanStep.new(index, state) \
          if statement.requires_delete?
        update_steps << InsertPlanStep.new(index, state) \
          if statement.requires_insert?

        UpdatePlan.new statement, index, plans, update_steps, @cost_model
      end.compact
    end
  end
end
