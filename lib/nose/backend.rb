module NoSE::Backend
  # Superclass of all database backends
  class BackendBase
    def initialize(workload, indexes, plans, _config)
      @workload = workload
      @indexes = indexes
      @plans = plans
    end

    # @abstract Subclasses implement to allow inserting
    #           data into the backend database
    # :nocov:
    def index_insert_chunk(index, chunk)
      raise NotImplementedError
    end
    # :nocov:

    # Execute a query with the stored plans
    def query(query)
      plan = @plans.find { |possible_plan| possible_plan.query == query }
      fail PlanNotFound if plan.nil?

      results = nil
      first_step = RootPlanStep.new QueryState.new(query, @workload)
      steps = [first_step] + plan.to_a + [nil]
      steps.each_cons(3) do |prev_step, step, next_step|
        step_class = QueryStep.subtype_class step.subtype_name

        # Check if the subclass has overridden this step
        subclass_step_name = step_class.name.sub 'NoSE::Backend',
                                                 self.class.name
        step_class = Object.const_get subclass_step_name

        results = step_class.process client, query, results,
                                     step, prev_step, next_step
      end

      results
    end

    # Superclass for all query execution steps
    class QueryStep
      include Supertype
    end

    # Look up data on an index in the backend
    class IndexLookupQueryStep < QueryStep
      # @abstract Subclasses should return an array of row hashes
      # :nocov:
      def self.process(_client, _query, _results, _step,
                       _prev_step, _next_step)
        fail NotImplementedError, 'Must be provided by a subclass'
      end
      # :nocov:
    end

    # Perform filtering external to the backend
    class FilterQueryStep < QueryStep
      # Filter results by a list of fields given in the step
      def self.process(_client, query, results, step,
                       _prev_step, _next_step)
        # Extract the equality conditions
        eq_conditions = query.conditions.select do |condition|
          !condition.range? && step.eq.include?(condition.field)
        end

        # XXX: This assumes that the range filter step is the same as
        #      the one in the query, which is always true for now
        range = step.range && query.conditions.find(&:range?)

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
    class SortQueryStep < QueryStep
      # Sort results by a list of fields given in the step
      def self.process(_client, _query, results, step, _prev_step, _next_step)
        results.sort_by! do |row|
          step.sort_fields.map do |field|
            row[field.id]
          end
        end
      end
    end
  end

  # Raised when a query is executed that we have no plan for
  class PlanNotFound < StandardError
  end
end
