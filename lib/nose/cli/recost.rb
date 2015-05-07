module NoSE::CLI
  # Add a command to recalculate costs with a new model
  class NoSECLI < Thor
    desc 'recost PLAN_FILE COST_MODEL',
         'recost the workload in PLAN_FILE with COST_MODEL'
    def recost(plan_file, cost_model)
      result = load_results plan_file
      config = load_config

      # Get the new cost model and recost all the queries
      new_cost_model = get_class 'cost', cost_model
      result.plans.each { |plan| plan.cost_model = new_cost_model }

      plans = Hash[result.plans.map { |plan| [plan.statement, plan] }]

      result.cost_model = new_cost_model
      total_cost = result.workload.statement_weights.map do |statement, weight|
        next 0 unless statement.is_a? NoSE::Query
        weight * plans[statement].cost
      end.inject(0, &:+)
      result.total_cost = total_cost

      output_json result
    end
  end
end
