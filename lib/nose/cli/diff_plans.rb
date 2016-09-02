# frozen_string_literal: true

module NoSE
  module CLI
    # Add a command to generate a graphic of the schema from a workload
    class NoSECLI < Thor
      desc 'diff-plans PLAN1 PLAN2',
           'output the differing plans between PLAN1 and PLAN2'

      long_desc <<-LONGDESC
        `nose diff-plans` loads two sets of statement plans generated
        separately by `nose search` and outputs the plans which are different.
      LONGDESC

      def diff_plans(plan1, plan2)
        result1 = load_results plan1
        result2 = load_results plan2

        output_diff plan1, result1, result2
        output_diff plan2, result2, result1
      end

      private

      # Output differing plans between two sets of results
      # @return [void]
      def output_diff(plan_name, result1, result2)
        puts Formatador.parse("[blue]#{plan_name}\n" + '━' * 50 + '[/]')
        plans1 = result1.plans.reject { |p| result2.plans.include?(p) }
        output_plans_txt plans1, $stdout, 1, result1.workload.statement_weights
        plans1 = result1.update_plans.reject do |plan|
          result2.update_plans.include? plan
        end
        output_update_plans_txt plans1, $stdout,
                                result1.workload.statement_weights
      end
    end
  end
end
