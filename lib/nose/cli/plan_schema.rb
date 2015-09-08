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
      option :mix, type: :string, default: 'default',
                   banner: 'the name of the workload mix for weighting queries'
      def plan_schema(workload_name, schema_name)
        workload = Workload.load workload_name
        workload.mix = options[:mix].to_sym \
          unless options[:mix] == 'default' && workload.mix != :default
        schema = Schema.load schema_name
        indexes = schema.indexes.values

        # Output the indexes
        header = "Indexes\n" + '━' * 50
        output_indexes_txt header, indexes, $stdout

        # Output the query plans
        cost_model = get_class_from_config options, 'cost', :cost_model
        planner = Plans::QueryPlanner.new workload, indexes, cost_model
        trees = workload.queries.map { |q| planner.find_plans_for_query q }
        plans = trees.map(&:min)
        output_plans_txt plans, $stdout, 1

        # Output the update plans
        planner = Plans::UpdatePlanner.new workload.model, trees, cost_model
        update_plans = []
        workload.statements.each do |statement|
          next if statement.is_a? Query

          planner.find_plans_for_update(statement, indexes).each do |plan|
            plan.select_query_plans(indexes)
            update_plans << plan
          end
        end
        output_update_plans_txt update_plans, $stdout,
                                workload.statement_weights
      end
    end
  end
end
