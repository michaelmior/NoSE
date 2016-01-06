module NoSE
  module Plans
    # Simple DSL for constructing execution plans
    class ExecutionPlans
      attr_reader :groups, :weights, :schema, :mix

      def initialize(&block)
        @groups = Hash.new { |h, k| h[k] = [] }
        @weights = Hash.new { |h, k| h[k] = {} }
        @mix = :default

        instance_eval(&block) if block_given?

        # Reset the mix to force weight assignment
        self.mix = @mix
      end

      # Find the plans with the given name
      def self.load(name)
        filename = File.expand_path "../../../../plans/#{name}.rb", __FILE__
        contents = File.read(filename)
        binding.eval contents, filename
      end

      # Populate the cost of each plan
      def calculate_cost(cost_model)
        @groups.each_value do |plans|
          plans.each do |plan|
            plan.steps.each do |step|
              cost = cost_model.index_lookup_cost step
              step.instance_variable_set(:@cost, cost)
            end

            plan.query_plans.each do |query_plan|
              query_plan.steps.each do |step|
                cost = cost_model.index_lookup_cost step
                step.instance_variable_set(:@cost, cost)
              end
            end

            # XXX Only bother with insert statements for now
            plan.update_steps.each do |step|
              cost = cost_model.insert_cost step
              step.instance_variable_set(:@cost, cost)
            end
          end
        end
      end

      # Set the weights on plans when the mix is changed
      def mix=(mix)
        @mix = mix

        @groups.each do |group, plans|
          plans.each do |plan|
            plan.instance_variable_set :@weight, @weights[group][@mix]
            plan.query_plans.each do |query_plan|
              query_plan.instance_variable_set :@weight, @weights[group][@mix]
            end
          end
        end
      end

      # rubocop:disable MethodName

      # Set the schema to be used by the execution plans
      def Schema(name)
        @schema = Schema.load name
        NoSE::DSL.mixin_fields @schema.model.entities, QueryExecutionPlan
        NoSE::DSL.mixin_fields @schema.model.entities, ExecutionPlans
      end

      # Define a group of query execution plans
      def Group(name, weight = 1.0, **mixes, &block)
        @group = name

        # Save the weights
        if mixes.empty?
          @weights[name][:default] = weight
        else
          @weights[name] = mixes
        end

        instance_eval(&block) if block_given?
      end

      # Define a single plan within a group
      def Plan(name, &block)
        return unless block_given?

        plan = QueryExecutionPlan.new(@group, name, @schema, self)

        # Capture one level of nesting in plans
        if @parent_plan.nil?
          @parent_plan = plan if @parent_plan.nil?
          set_parent = true
        else
          set_parent = false
        end

        plan.instance_eval(&block)

        # Reset the parent plan if it was set
        @parent_plan = nil if set_parent

        @groups[@group] << plan
      end

      # Add support queries for updates in a plan
      def Support(&block)
        # XXX Hack to swap the group name and capture support plans
        old_group = @group
        @group = '__SUPPORT__'
        instance_eval(&block) if block_given?

        @parent_plan.query_plans = @groups[@group]
        @parent_plan.query_plans.each do |plan|
          plan.instance_variable_set(:@group, old_group)
        end

        @groups[@group] = []

        @group = old_group
      end

      # rubocop:enable MethodName
    end

    # DSL to construct query execution plans
    class QueryExecutionPlan < AbstractPlan
      attr_reader :group, :name, :params, :select_fields,
                  :steps, :update_steps, :index
      attr_accessor :query_plans

      # Most of the work is delegated to the array
      extend Forwardable
      def_delegators :@steps, :each, :<<, :[], :==, :===, :eql?,
                     :inspect, :to_s, :to_a, :to_ary, :last, :length, :count

      def initialize(group, name, schema, plans)
        @group = group
        @name = name
        @schema = schema
        @plans = plans
        @select_fields = []
        @params = {}
        @steps = []
        @update_steps = []
        @query_plans = []
      end

      # These plans have no associated query
      def query
        nil
      end

      # The estimated cost of executing this plan
      def cost
        costs = @steps.map(&:cost) + @update_steps.map(&:cost)
        costs += @query_plans.map(&:steps).flatten.map(&:cost)

        costs.inject(0, &:+)
      end

      # rubocop:disable MethodName

      # Identify fields to be selected
      def Select(*fields)
        @select_fields = fields.flatten.to_set
      end

      # Add parameters which are used as input to the plan
      def Param(field, operator, value = nil)
        operator = :'=' if operator == :==
        @params[field.id] = Condition.new(field, operator, value)
      end

      # Pass the support query up to the parent
      def Support(&block)
        @plans.Support(&block)
      end

      # Create a new index lookup step with a particular set of conditions
      def Lookup(index_key, *conditions, limit: nil)
        index = @schema.indexes[index_key]

        step = Plans::IndexLookupPlanStep.new index
        eq_fields = Set.new
        range_field = nil
        conditions.each do |field, operator|
          if operator == :==
            eq_fields.add field
          else
            range_field = field
          end
        end

        step.instance_variable_set :@eq_filter, eq_fields
        step.instance_variable_set :@range_filter, range_field

        # XXX No ordering supported for now
        step.instance_variable_set :@order_by, []

        step.instance_variable_set :@limit, limit unless limit.nil?

        # Cardinality calculations adapted from
        # IndexLookupPlanStep#update_state
        state = OpenStruct.new
        if @steps.empty?
          state.hash_cardinality = 1
        else
          state.hash_cardinality = @steps.last.state.cardinality
        end
        cardinality = index.per_hash_count * state.hash_cardinality
        state.cardinality = Cardinality.filter cardinality,
                                               eq_fields - index.hash_fields,
                                               range_field

        step.state = state

        @steps << step
      end

      # Add a new insertion step into an index
      def Insert(index_key, *fields)
        @index = @schema.indexes[index_key]

        step = Plans::InsertPlanStep.new @index
        fields = @index.all_fields if fields.empty?
        step.instance_variable_set(:@fields, fields)

        # Get cardinality from last step of each support query plan
        # as in UpdatePlanner#find_plans_for_update
        cardinalities = @query_plans.map { |p| p.steps.last.state.cardinality }
        cardinality = cardinalities.inject(1, &:*)
        step.state = OpenStruct.new cardinality: cardinality

        @update_steps << step
      end

      # Add a new deletion step from an index
      def Delete(index_key)
        @index = @schema.indexes[index_key]

        step = Plans::DeletePlanStep.new @index

        # Get cardinality from last step of each support query plan
        # as in UpdatePlanner#find_plans_for_update
        cardinalities = @query_plans.map { |p| p.steps.last.state.cardinality }
        cardinality = cardinalities.inject(1, &:*)
        step.state = OpenStruct.new cardinality: cardinality

        @update_steps << step
      end

      # rubocop:enable MethodName
    end
  end
end
