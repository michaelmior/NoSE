module Sadvisor
  class Backend
    def initialize(workload, indexes, plans, config)
      @workload = workload
      @indexes = indexes
      @plans = plans
    end

    # Execute a query with the stored plans
    def query(query)
      plan = @plans.find { |possible_plan| possible_plan.query == query }

      results = nil
      first_step = RootPlanStep.new QueryState.new(query, @workload)
      steps = [first_step] + plan.to_a + [nil]
      steps.each_cons(3) do |prev_step, step, next_step|
        step_class = QueryStep.subtype_class step.subtype_name

        # Check if the subclass has overridden this step
        subclass_step_name = step_class.name.sub 'Sadvisor::Backend',
                                                 self.class.name
        step_class = Object.const_get subclass_step_name

        results = step_class.process client, query, results,
                                     step, prev_step, next_step
      end

      results
    end

    private

    class QueryStep
      include Supertype
    end

    class IndexLookupQueryStep < QueryStep
      def self.process(client, query, results, step, prev_step, next_step)
        fail NotImplementedError.new('Must be provided by a subclass')
      end
    end

    class FilterQueryStep < QueryStep
      def self.process(client, query, results, step, prev_step, next_step)
        fail NotImplementedError, 'Filtering is not yet implemented'
      end
    end

    class SortQueryStep < QueryStep
      def self.process(client, query, results, step, prev_step, next_step)
        results.sort_by! do |row|
          sort = step.sort_fields.map do |field|
            row["#{field.parent.name}_#{field.name}"]
          end
        end
      end
    end
  end
end
