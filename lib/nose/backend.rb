module NoSE
  # Communication with backends for index creation and statement execution
  module Backend
    # Superclass of all database backends
    class BackendBase
      def initialize(workload, indexes, plans, update_plans, _config)
        @workload = workload
        @indexes = indexes
        @plans = plans
        @update_plans = update_plans
      end

      # The base shows all indexes as empty for testing purposes
      def index_empty?(_index)
        true
      end

      # @abstract Subclasses implement to allow inserting
      #           data into the backend database
      # :nocov:
      def index_insert_chunk(_index, _chunk)
        fail NotImplementedError
      end
      # :nocov:

      # @abstract Subclasses implement to generate a new random ID
      # :nocov:
      def generate_id
        fail NotImplementedError
      end
      # :nocov:

      # Prepare a query to be executed with the given plans
      def prepare_query(query, fields, conditions, plans = [])
        if plans.empty?
          plan = @plans.find do |possible_plan|
            possible_plan.query == query
          end unless query.nil?
          fail PlanNotFound if plan.nil?
        else
          plan = plans.first
        end

        state = Plans::QueryState.new(query, @workload) unless query.nil?
        first_step = Plans::RootPlanStep.new state
        steps = [first_step] + plan.to_a + [nil]
        exec_steps = steps.each_cons(3).map do |prev_step, step, next_step|
          step_class = StatementStep.subtype_class step.subtype_name

          # Check if the subclass has overridden this step
          subclass_step_name = step_class.name.sub \
            'NoSE::Backend::BackendBase', self.class.name
          step_class = Object.const_get subclass_step_name
          step_class.new client, fields, conditions,
                         step, next_step, prev_step
        end

        PreparedQuery.new query, exec_steps
      end

      # Prepare a statement to be executed with the given plans
      def prepare(statement, plans = [])
        if statement.is_a? Query
          prepared = prepare_query statement, statement.all_fields,
                                   statement.conditions, plans
        else
          prepared = prepare_update statement, update.settings, plans
        end

        prepared
      end

      # Execute a query with the stored plans
      def query(query, plans = nil)
        prepared = prepare query, plans
        prepared.execute query.conditions
      end

      # Prepare an update for execution
      def prepare_update(update, update_settings, plans)
        # Search for plans if they were not given
        if plans.empty?
          plans = @update_plans.select do |possible_plan|
            possible_plan.statement == update
          end
        end
        fail PlanNotFound if plans.empty?

        # Prepare each plan
        plans.map do |plan|
          delete = false
          insert = false
          plan.update_steps.each do |step|
            delete = true if step.is_a?(Plans::DeletePlanStep)
            insert = true if step.is_a?(Plans::InsertPlanStep)
          end

          steps = []
          if delete
            step_class = DeleteStatementStep
            subclass_step_name = step_class.name.sub \
              'NoSE::Backend::BackendBase', self.class.name
            step_class = Object.const_get subclass_step_name
            steps << step_class.new(client, plan.index)
          end
          return PreparedUpdate.new update, [], steps if delete && !insert

          if insert
            step_class = InsertStatementStep
            subclass_step_name = step_class.name.sub \
              'NoSE::Backend::BackendBase', self.class.name
            step_class = Object.const_get subclass_step_name
            steps << step_class.new(client, plan.index,
                                    update_settings.map(&:field))
          end

          # Prepare plans for each support query
          support_plans = plan.query_plans.map do |query_plan|
            query = query_plan.instance_variable_get(:@query)
            prepare_query query, query_plan.select_fields, query_plan.params,
                          [query_plan.steps]
          end

          PreparedUpdate.new update, support_plans, steps
        end
      end

      # Execute an update with the stored plans
      def update(update, plans = [])
        prepared = prepare_update update, update.settings, plans
        prepared.each { |p| p.execute update.settings, update.conditions }
      end

      # Superclass for all statement execution steps
      class StatementStep
        include Supertype
        attr_reader :index
      end

      # Look up data on an index in the backend
      class IndexLookupStatementStep < StatementStep
      end

      # Insert data into an index on the backend
      class InsertStatementStep < StatementStep
      end

      # Delete data from an index on the backend
      class DeleteStatementStep < StatementStep
      end

      # Perform filtering external to the backend
      class FilterStatementStep < StatementStep
        def initialize(_client, _query, step, _next_step, _prev_step)
          @step = step
        end

        # Filter results by a list of fields given in the step
        def process(conditions, results)
          # Extract the equality conditions
          eq_conditions = conditions.values.select do |condition|
            !condition.range? && @step.eq.include?(condition.field)
          end

          # XXX: This assumes that the range filter step is the same as
          #      the one in the query, which is always true for now
          range = @step.range && conditions.values.find(&:range?)

          results.select! do |row|
            select = eq_conditions.all? do |condition|
              row[condition.field.id] == condition.value
            end

            if range
              range_check = row[range.field.id].method(range.operator)
              select &&= range_check.call range.value
            end

            select
          end
        end
      end

      # Perform sorting external to the backend
      class SortStatementStep < StatementStep
        def initialize(_client, _query, step, _next_step, _prev_step)
          @step = step
        end

        # Sort results by a list of fields given in the step
        def process(_conditions, results)
          results.sort_by! do |row|
            @step.sort_fields.map do |field|
              row[field.id]
            end
          end
        end
      end
    end

    # A prepared query which can be executed against the backend
    class PreparedQuery
      attr_reader :query, :steps

      def initialize(query, steps)
        @query = query
        @steps = steps
      end

      # Execute the query for the given set of conditions
      def execute(conditions)
        results = nil

        @steps.each do |step|
          results = step.process conditions, results

          # The query can't return any results at this point, so we're done
          break if results.empty?
        end

        results
      end
    end

    # An update prepared with a backend which is ready to execute
    class PreparedUpdate
      attr_reader :statement, :steps

      def initialize(statement, support_plans, steps)
        @statement = statement
        @support_plans = support_plans
        @delete_step = steps.find do |step|
          step.is_a? BackendBase::DeleteStatementStep
        end
        @insert_step = steps.find do |step|
          step.is_a? BackendBase::InsertStatementStep
        end
      end

      # Execute the statement for the given set of conditions
      def execute(update_settings, update_conditions)
        # Execute all the support queries
        settings = initial_update_settings update_settings, update_conditions

        if !@delete_step.nil? && @insert_step.nil?
          # Populate the data to remove for deletions
          support = [Hash[update_conditions.map do |field_id, condition|
            [field_id, condition.value]
          end]]
        else
          # Execute the support queries for this update
          support = support_results update_conditions
        end

        # Perform the deletion
        @delete_step.process support unless support.empty? || @delete_step.nil?
        return if @insert_step.nil?

        if support.empty?
          support = [settings]
        else
          support.each { |row| row.merge! settings }
        end

        # Stop if we have nothing to insert, otherwise insert
        return if support.empty?
        @insert_step.process support
      end

      private

      # Get the initial values which will be used in the first plan step
      def initial_update_settings(update_settings, update_conditions)
        if !@insert_step.nil? && @delete_step.nil?
          # Populate the data to insert for Insert statements
          settings = Hash[update_settings.map do |setting|
            [setting.field.id, setting.value]
          end]
        else
          # Get values for updates and deletes
          settings = Hash[update_conditions.map do |field_id, condition|
            [field_id, condition.value]
          end]
        end

        settings
      end

      # Execute all the support queries
      def support_results(settings)
        support = @support_plans.map do |query_plan|
          query_plan.execute settings
        end

        # Combine the results from multiple support queries
        if support.length > 0
          support = support.first.product(*support[1..-1])
          support.map! { |results| results.reduce(&:merge) }
        end

        support
      end
    end

    # Raised when a statement is executed that we have no plan for
    class PlanNotFound < StandardError
    end

    # Raised when a backend attempts to create an index that already exists
    class IndexAlreadyExists < StandardError
    end
  end
end
