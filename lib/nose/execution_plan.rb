module NoSE
  # Simple DSL for constructing execution plans
  class ExecutionPlans
    attr_reader :plans, :schema

    def initialize(&block)
      @plans = []
      instance_eval(&block) if block_given?
    end

    # Find the plans with the given name
    def self.load(name)
      filename = File.expand_path "../../../plans/#{name}.rb", __FILE__
      contents = File.read(filename)
      binding.eval contents, filename
    end

    # rubocop:disable MethodName

    # Set the schema to be used by the execution plans
    def Schema(name)
      @schema = Schema.load name
      NoSE::DSL.mixin_fields @schema.workload.model.entities,
                             QueryExecutionPlan
    end

    # Define a group of query execution plans
    def Group(_name, _weight = 1.0, **_mixes, &block)
      # XXX Groups are basically ignored for now
      instance_eval(&block) if block_given?
    end

    # Define a single plan within a group
    def Plan(&block)
      return unless block_given?

      plan = QueryExecutionPlan.new(@schema)
      plan.instance_eval(&block)
      @plans << plan
    end

    # rubocop:enable MethodName
  end

  # DSL to construct query execution plans
  class QueryExecutionPlan
    attr_reader :params, :select, :steps

    def initialize(schema)
      @schema = schema
      @select = []
      @params = {}
      @steps = []
    end

    # rubocop:disable MethodName

    # Identify fields to be selected
    def Select(*fields)
      @select = fields.flatten.to_set
    end

    # Add parameters which are used as input to the plan
    def Param(field, operator, value = nil)
      operator = :'=' if operator == :==
      @params[field.id] = Condition.new(field, operator, value)
    end

    # Create a new index lookup step with a particular set of conditions
    def Lookup(index_key, *conditions, limit: nil)
      index = @schema.indexes[index_key]

      step = Plans::IndexLookupPlanStep.new index
      eq_fields = []
      range_field = nil
      conditions.each do |field, operator|
        if operator == :==
          eq_fields.push field
        else
          range_field = field
        end
      end

      step.instance_variable_set :@eq_filter, eq_fields
      step.instance_variable_set :@range_filter, range_field

      # XXX No ordering supported for now
      step.instance_variable_set :@order_by, []

      step.instance_variable_set :@limit, limit unless limit.nil?

      @steps << step
    end

    # rubocop:enable MethodName
  end
end
