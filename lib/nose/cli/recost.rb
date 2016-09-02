# frozen_string_literal: true

module NoSE
  module CLI
    # Add a command to recalculate costs with a new model
    class NoSECLI < Thor
      desc 'recost PLAN_FILE COST_MODEL',
           'recost the workload in PLAN_FILE with COST_MODEL'

      long_desc <<-LONGDESC
        `nose recost` takes a given plan file as input and then produces a new
        JSON file with new values for the expected query cost from the named
        cost model. This only works for cost models which don't require any
        parameters such as `Cost::RequestCountCost`.
      LONGDESC

      def recost(plan_file, cost_model)
        result = load_results plan_file

        # Get the new cost model and recost all the queries
        new_cost_model = get_class('cost', cost_model).new
        result.plans.each { |plan| plan.cost_model = new_cost_model }

        # Update the cost values
        result.cost_model = new_cost_model
        result.total_cost = total_cost result.workload, result.plans

        output_json result
      end

      private

      # Get the total cost for a set of plans in a given workload
      # @return [Fixnum]
      def total_cost(workload, plans)
        plan_hash = Hash[plans.map { |plan| [plan.query, plan] }]

        workload.statement_weights.map do |statement, weight|
          next 0 unless statement.is_a? Query
          weight * plan_hash[statement].cost
        end.inject(0, &:+)
      end
    end
  end
end
