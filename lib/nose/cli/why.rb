module NoSE
  module CLI
    # Add a command to generate a graphic of the schema from a workload
    class NoSECLI < Thor
      desc 'why PLAN_FILE',
           'output the reason for including each index in PLAN_FILE'

      long_desc <<-LONGDESC
        `nose why` is used to better understand why NoSE included a particular
        index in a schema. This is especially helpful when comparing with
        manually-defined execution plans.
      LONGDESC

      def why(plan_file)
        result = load_results plan_file

        query_count = Set.new
        support_count = Set.new
        indexes_usage = Hash.new { |h, k| h[k] = [] }

        # Count the indexes used in queries
        result.plans.each do |plan|
          plan.indexes.each do |index|
            indexes_usage[index] << plan.query
            query_count.add index
          end
        end

        # Count the indexes used in support queries
        result.update_plans.each do |plan|
          plan.query_plans.each do |query_plan|
            query_plan.indexes.each do |index|
              indexes_usage[index] << plan.statement
              support_count.add index unless query_count.include? index
            end
          end
        end

        # Print out the statements each index is used for
        indexes_usage.each do |index, statements|
          p index
          statements.each { |s| p s }
          puts
        end

        # Put the number of indexes used for queries and support queries
        puts "        Queries: #{query_count.length}"
        puts "Support queries: #{support_count.length}"
      end
    end
  end
end
