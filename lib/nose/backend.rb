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

      # @abstract Subclasses implement to allow inserting
      #           data into the backend database
      # :nocov:
      def index_insert_chunk(_index, _chunk)
        fail NotImplementedError
      end
      # :nocov:

      # Prepare a statement to be executed with the given plans
      def prepare(statement, plans = [])
        if statement.is_a? Query
          if plans.empty?
            plan = @plans.find do |possible_plan|
              possible_plan.query == statement
            end
            fail PlanNotFound if plan.nil?
          else
            plan = plans.first
          end

          first_step = Plans::RootPlanStep.new \
            Plans::QueryState.new(statement, @workload)
          steps = [first_step] + plan.to_a + [nil]
          exec_steps = steps.each_cons(3).map do |prev_step, step, next_step|
            step_class = StatementStep.subtype_class step.subtype_name

            # Check if the subclass has overridden this step
            subclass_step_name = step_class.name.sub \
              'NoSE::Backend::BackendBase', self.class.name
            step_class = Object.const_get subclass_step_name
            step_class.new client, statement.all_fields, statement.conditions,
                           step, next_step, prev_step
          end

          prepared = PreparedQuery.new statement, exec_steps
        else
          fail NotImplementedError
        end

        prepared
      end

      # Execute a query with the stored plans
      def query(query, plans = nil)
        prepared = prepare query, plans
        prepared.execute query.conditions
      end

      # Execute an update with the stored plans
      def update(update, plans = [])
        # Search for plans if they were not given
        if plans.empty?
          plans = @update_plans.select do |possible_plan|
            possible_plan.statement == update
          end
        end
        fail PlanNotFound if plans.empty?

        plans.each { |plan| execute_update_plan update, plan }
      end

      # Superclass for all statement execution steps
      class StatementStep
        include Supertype
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

      private

      # Execute a single update plan as part of an update
      def execute_update_plan(update, plan)
        # Execute all the support queries
        settings = initial_update_settings update
        support = support_results plan.query_plans, settings

        # Populate the data to remove for Delete statements
        if update.is_a? Delete
          support = [Hash[update.conditions.map do |field_id, condition|
            [field_id, condition.value]
          end]]
        end

        if update.requires_delete?
          step_class = DeleteStatementStep
          subclass_step_name = step_class.name.sub \
            'NoSE::Backend::BackendBase', self.class.name
          step_class = Object.const_get subclass_step_name
          prepared = step_class.new client, plan.index
          prepared.process support unless support.empty?
        end
        return if update.is_a? Delete

        if update.is_a? Insert
          support = [settings]
        else
          support.each { |row| row.merge! settings }
        end

        # Stop if we have nothing to insert
        return if support.empty?

        step_class = InsertStatementStep
        subclass_step_name = step_class.name.sub \
          'NoSE::Backend::BackendBase', self.class.name
        step_class = Object.const_get subclass_step_name
        prepared = step_class.new client, index, results.first.keys.sort
        prepared.process support
      end

      # Get the initial values which will be used in the first plan step
      def initial_update_settings(update)
        if update.is_a? Insert
          # Populate the data to insert for Insert statements
          settings = Hash[update.settings.map do |setting|
            [setting.field.id, setting.value]
          end]
        elsif update.is_a? Connection
          # Populate the data to insert for Connection statements
          settings = {
            update.source.id_fields.first.id => update.source_pk,
            update.target.entity.id_fields.first.id => update.target_pk
          }
        else
          # Get values for updates and deletes
          settings = Hash[update.conditions.map do |field_id, condition|
            [field_id, condition.value]
          end]
        end

        settings
      end

      # Execute all the support queries
      def support_results(query_plans, settings)
        support = query_plans.map do |query_plan|
          execute_support_query query_plan, settings
        end

        # Combine the results from multiple support queries
        if support.length > 0
          support = support.first.product(*support[1..-1])
          support.map! { |results| results.reduce(&:merge) }
        end

        support
      end

      # Execute a support query for an update
      def execute_support_query(query_plan, settings)
        # Substitute values into the support query
        support_query = query_plan.query.dup
        support_query.conditions.each do |field_id, condition|
          condition.instance_variable_set :@value, settings[field_id]
        end

        # Execute the support query and return the results
        query(support_query, query_plan)
      end
    end

    # A prepared query which can be executed against the backend
    class PreparedQuery
      attr_reader :query

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

    # Raised when a statement is executed that we have no plan for
    class PlanNotFound < StandardError
    end

    # Raised when a backend attempts to create an index that already exists
    class IndexAlreadyExists < StandardError
    end
  end
end
