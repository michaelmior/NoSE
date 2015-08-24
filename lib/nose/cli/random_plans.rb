module NoSE
  module CLI
    # Add a command to generate a graphic of the schema from a workload
    class NoSECLI < Thor
      desc 'random_plans WORKLOAD TAG',
           'output random plans for the statement with TAG in WORKLOAD'
      long_desc <<-LONGDESC
        `nose random-plans` produces a set of randomly chosen plans for a given
        workload. This is useful when evaluating the cost model for a given
        system as this should give a good range of different execution plans.
      LONGDESC
      option :count, type: :numeric, default: 5, aliases: '-n',
                     banner: 'the number of random plans to produce'
      option :all, type: :boolean, default: false, aliases: '-a',
                   banner: 'whether to include all possible plans, if given ' \
                           'then --count is ignored'
      option :format, type: :string, default: 'txt',
                      enum: %w(txt json yml), aliases: '-f',
                      banner: 'the format of the produced plans'
      option :output, type: :string, default: nil, aliases: '-o',
                      banner: 'a file where produced plans should be stored'
      def random_plans(workload_name, tag)
        workload = Workload.load workload_name

        # Find the statement with the given tag
        statement = workload.statements.find do |s|
          s.text.end_with? "-- #{tag}"
        end

        # Generate a random set of plans
        indexes = IndexEnumerator.new(workload).indexes_for_workload
        cost_model = get_class('cost', options[:cost_model][:name]).new(**options[:cost_model])
        planner = Plans::QueryPlanner.new workload, indexes, cost_model
        plans = planner.find_plans_for_query(statement).to_a
        plans = plans.sample options[:count] unless options[:all]

        results = OpenStruct.new
        results.workload = workload
        results.enumerated_indexes = indexes
        results.indexes = plans.map(&:indexes).flatten(1).to_set
        results.plans = plans
        results.cost_model = cost_model

        # Output the results in the specified format
        file = options[:output].nil? ? $stdout :
                                       File.open(options[:output], 'w')
        begin
          send(('output_' + options[:format]).to_sym, results, file, true)
        ensure
          file.close unless options[:output].nil?
        end
      end
    end
  end
end
