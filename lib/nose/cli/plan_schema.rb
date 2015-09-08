module NoSE
  module CLI
    # Add a command to generate a graphic of the schema from a workload
    class NoSECLI < Thor
      desc 'plan-schema WORKLOAD SCHEMA',
           'output plans for the given WORKLOAD using SCHEMA'
      long_desc <<-LONGDESC
        `nose plan-schema`
      LONGDESC
      def plan_schema(workload_name, schema_name)
        workload = Workload.load workload_name
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
      end
    end
  end
end
