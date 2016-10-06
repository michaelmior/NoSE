# frozen_string_literal: true

module NoSE
  module CLI
    # Add a command to generate a graphic of the schema from a workload
    class NoSECLI < Thor
      desc 'random-plans WORKLOAD TAG',
           'output random plans for the statement with TAG in WORKLOAD'

      long_desc <<-LONGDESC
        `nose random-plans` produces a set of randomly chosen plans for a given
        workload. This is useful when evaluating the cost model for a given
        system as this should give a good range of different execution plans.
      LONGDESC

      shared_option :format
      shared_option :output

      option :count, type: :numeric, default: 5, aliases: '-n',
                     desc: 'the number of random plans to produce'
      option :all, type: :boolean, default: false, aliases: '-a',
                   desc: 'whether to include all possible plans, if given ' \
                         'then --count is ignored'

      def random_plans(workload_name, tag)
        # Find the statement with the given tag
        workload = Workload.load workload_name
        statement = workload.find_with_tag tag

        # Generate a random set of plans
        indexes = IndexEnumerator.new(workload).indexes_for_workload
        cost_model = get_class('cost', options[:cost_model][:name]) \
                     .new(**options[:cost_model])
        plans = find_random_plans statement, workload, indexes, cost_model,
                                  options
        results = random_plan_results workload, indexes, plans, cost_model
        output_random_plans results, options[:output], options[:format]
      end

      private

      # Generate random plans for a givne statement
      # @return [Array]
      def find_random_plans(statement, workload, indexes, cost_model, options)
        planner = Plans::QueryPlanner.new workload, indexes, cost_model
        plans = planner.find_plans_for_query(statement).to_a
        plans = plans.sample options[:count] unless options[:all]

        plans
      end

      # Build a results structure for these plans
      # @return [OpenStruct]
      def random_plan_results(workload, indexes, plans, cost_model)
        results = OpenStruct.new
        results.workload = workload
        results.model = workload.model
        results.enumerated_indexes = indexes
        results.indexes = plans.map(&:indexes).flatten(1).to_set
        results.plans = plans
        results.cost_model = cost_model

        results
      end

      # Output the results in the specified format
      # @return [void]
      def output_random_plans(results, output, format)
        if output.nil?
          file = $stdout
        else
          File.open(output, 'w')
        end
        begin
          send(('output_' + format).to_sym, results, file, true)
        ensure
          file.close unless output.nil?
        end
      end
    end
  end
end
