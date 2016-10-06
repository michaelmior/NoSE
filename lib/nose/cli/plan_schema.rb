# frozen_string_literal: true

module NoSE
  module CLI
    # Add a command to generate a graphic of the schema from a workload
    class NoSECLI < Thor
      desc 'plan-schema WORKLOAD SCHEMA',
           'output plans for the given WORKLOAD using SCHEMA'

      long_desc <<-LONGDESC
        `nose plan-schema` produces a set of plans for the given WORKLOAD
        using the manually-defined SCHEMA.

        This is useful to compare manually-defined execution plans with the
        plans that NoSE would produce for the same schema.
      LONGDESC

      shared_option :format
      shared_option :mix

      def plan_schema(workload_name, schema_name)
        workload = Workload.load workload_name
        workload.mix = options[:mix].to_sym \
          unless options[:mix] == 'default' && workload.mix != :default
        schema = Schema.load schema_name
        indexes = schema.indexes.values

        # Build the statement plans
        cost_model = get_class_from_config options, 'cost', :cost_model
        planner = Plans::QueryPlanner.new workload, indexes, cost_model
        trees = workload.queries.map { |q| planner.find_plans_for_query q }
        plans = trees.map(&:min)

        update_plans = build_update_plans workload.statements, indexes,
                                          workload.model, trees, cost_model

        # Construct a result set
        results = plan_schema_results workload, indexes, plans, update_plans,
                                      cost_model

        # Output the results in the specified format
        send(('output_' + options[:format]).to_sym, results)
      end

      private

      # Construct a result set
      # @return [OpenStruct]
      def plan_schema_results(workload, indexes, plans, update_plans,
                              cost_model)
        results = OpenStruct.new
        results.workload = workload
        results.model = workload.model
        results.indexes = indexes
        results.enumerated_indexes = []
        results.plans = plans
        results.update_plans = update_plans
        results.cost_model = cost_model
        results.weights = workload.statement_weights
        results.total_size = results.indexes.sum_by(&:size)
        results.total_cost = plans.sum_by { |plan| plan.cost * plan.weight }

        results
      end

      # Produce all update plans for the schema
      # @return [Array<Plans::UpdatePlan>]
      def build_update_plans(statements, indexes, model, trees, cost_model)
        planner = Plans::UpdatePlanner.new model, trees, cost_model
        update_plans = []
        statements.each do |statement|
          next if statement.is_a? Query

          planner.find_plans_for_update(statement, indexes).each do |plan|
            plan.select_query_plans(indexes)
            update_plans << plan
          end
        end

        update_plans
      end
    end
  end
end
