# frozen_string_literal: true

module NoSE
  module Plans
    # A simple state class to hold the cardinality for updates
    class UpdateState
      attr_reader :statement, :cardinality

      def initialize(statement, cardinality)
        @statement = statement
        @cardinality = cardinality
      end

      # XXX This is just a placeholder since this is
      #     currently used by the query planner
      # @return [Boolean]
      def answered?
        true
      end
    end

    # A plan for executing an update
    class UpdatePlan < AbstractPlan
      attr_reader :statement, :index, :query_plans, :update_steps, :cost_model,
                  :update_fields

      include Comparable

      def initialize(statement, index, trees, update_steps, cost_model)
        @statement = statement
        @index = index
        @trees = trees
        @query_plans = nil # these will be set later when we pick indexes
        update_steps.each { |step| step.calculate_cost cost_model }
        @update_steps = update_steps
        @cost_model = cost_model

        # Update with fields specified in the settings and conditions
        # (rewrite from foreign keys to IDs if needed)
        @update_fields = if statement.is_a?(Connection) ||
                            statement.is_a?(Delete)
                           []
                         else
                           statement.settings.map(&:field)
                         end
        @update_fields += statement.conditions.each_value.map(&:field)
        @update_fields.map! do |field|
          field.is_a?(Fields::ForeignKeyField) ? field.entity.id_field : field
        end
      end

      # The weight of this query for a given workload
      # @return [Fixnum]
      def weight
        return 1 if @workload.nil?

        @workload.statement_weights[@statement]
      end

      # The group of the associated statement
      # @return [String]
      def group
        @statement.group
      end

      # Name the plan by the statement
      # @return [String]
      def name
        "#{@statement.text} for #{@index.key}"
      end

      # The steps for this plan are the update steps
      # @return [Array<UpdatePlanStep>]
      def steps
        @update_steps
      end

      # Parameters to this update plan
      # @return [Hash]
      def params
        conditions = if @statement.respond_to?(:conditions)
                       @statement.conditions
                     else
                       {}
                     end
        settings = if @statement.respond_to?(:settings)
                     @statement.settings
                   else
                     []
                   end

        params = conditions.merge Hash[settings.map do |setting|
          [setting.field.id, Condition.new(setting.field, :'=', setting.value)]
        end]

        convert_param_keys params
      end

      # Select query plans to actually use here
      # @return [void]
      def select_query_plans(indexes = nil, &block)
        if block_given?
          @query_plans = @trees.map(&block)
        else
          @query_plans = @trees.map do |tree|
            plan = tree.select_using_indexes(indexes).min_by(&:cost)
            fail if plan.nil?
            plan
          end
        end

        update_support_fields

        @trees = nil
      end

      # Compare all the fields for the plan for equality
      # @return [Boolean]
      def eql?(other)
        return false unless other.is_a? UpdatePlan
        fail 'plans must be resolved before checking equality' \
          if @query_plans.nil? || other.query_plans.nil?

        @statement == other.statement &&
          @index == other.index &&
          @query_plans == other.query_plans &&
          @update_steps == other.update_steps &&
          @cost_model == other.cost_model
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
      # @return [Boolean]
      def <=>(other)
        cost <=> other.cost
      end

      # The cost of performing the update on this index
      # @return [Fixnum]
      def update_cost
        @update_steps.sum_by(&:cost)
      end

      # The cost is the sum of all the query costs plus the update costs
      # @return [Fixnum]
      def cost
        @query_plans.sum_by(&:cost) + update_cost
      end

      private

      # Add fields from support queries to those which should be updated
      # @return [void]
      def update_support_fields
        # Add fields fetched from support queries
        @update_fields += @query_plans.flat_map do |query_plan|
          query_plan.query.select.to_a
        end.compact
      end

      # Ensure we only use primary keys for conditions
      # @return [Hash]
      def convert_param_keys(params)
        Hash[params.each_value.map do |condition|
          field = condition.field
          if field.is_a?(Fields::ForeignKeyField)
            field = field.entity.id_field
            condition = Condition.new field, condition.operator,
                                      condition.value
          end

          [field.id, condition]
        end]
      end
    end

    # A planner for update statements in the workload
    class UpdatePlanner
      def initialize(model, trees, cost_model, by_id_graph = false)
        @logger = Logging.logger['nose::update_planner']

        @model = model
        @cost_model = cost_model
        @by_id_graph = by_id_graph

        # Remove anything not a support query then group by statement and index
        @query_plans = trees.select do |tree|
          tree.query.is_a? SupportQuery
        end
        @query_plans = @query_plans.group_by { |tree| tree.query.statement }
        @query_plans.each do |plan_stmt, plan_trees|
          @query_plans[plan_stmt] = plan_trees.group_by do |tree|
            index = tree.query.index
            index = index.to_id_path if @by_id_path

            index
          end
        end
      end

      # Find the necessary update plans for a given set of indexes
      # @return [Array<UpdatePlan>]
      def find_plans_for_update(statement, indexes)
        indexes = indexes.map(&:to_id_graph).to_set if @by_id_graph

        indexes.map do |index|
          next unless statement.modifies_index?(index)

          if (@query_plans[statement] &&
              @query_plans[statement][index]).nil?
            trees = []

            if statement.is_a? Insert
              cardinality = 1
            else
              cardinality = Cardinality.filter index.entries,
                                               statement.eq_fields,
                                               statement.range_field
            end
          else
            # Get the cardinality of the last step to use for the update state
            trees = @query_plans[statement][index]
            plans = trees.map do |tree|
              tree.select_using_indexes(indexes).min_by(&:cost)
            end

            # Multiply the cardinalities because we are crossing multiple
            # relationships and need the cross-product
            cardinality = plans.product_by { |p| p.last.state.cardinality }
          end

          state = UpdateState.new statement, cardinality
          update_steps = update_steps statement, index, state
          UpdatePlan.new statement, index, trees, update_steps, @cost_model
        end.compact
      end

      private

      # Find the required update steps
      # @return [Array<UpdatePlanStep>]
      def update_steps(statement, index, state)
        update_steps = []
        update_steps << DeletePlanStep.new(index, state) \
          if statement.requires_delete?(index)

        if statement.requires_insert?(index)
          fields = if statement.is_a?(Connect)
                     statement.conditions.each_value.map(&:field)
                   else
                     statement.settings.map(&:field)
                   end

          update_steps << InsertPlanStep.new(index, state, fields)
        end

        update_steps
      end
    end
  end
end
