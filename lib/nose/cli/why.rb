# frozen_string_literal: true

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
        indexes_usage = Hash.new { |h, k| h[k] = [] }

        # Count the indexes used in queries
        query_count = Set.new
        update_index_usage result.plans, indexes_usage, query_count

        # Count the indexes used in support queries
        # (ignoring those used in queries)
        support_count = Set.new
        result.update_plans.each do |plan|
          update_index_usage plan.query_plans, indexes_usage,
                             support_count, query_count
        end

        # Produce the final output of index usage
        print_index_usage indexes_usage, query_count, support_count
      end

      private

      # Track usage of indexes in the set of query plans updating both
      # a dictionary of statements relevant to each index and a set
      # of unique statements used (optionally ignoring some)
      # @return [void]
      def update_index_usage(plans, indexes_usage, statement_usage,
                             ignore = Set.new)
        plans.each do |plan|
          plan.indexes.each do |index|
            indexes_usage[index] << if plan.respond_to?(:statement)
                                      plan.statement
                                    else
                                      plan.query
                                    end
            statement_usage.add index unless ignore.include? index
          end
        end
      end

      # Print out the statements each index is used for
      # @return [void]
      def print_index_usage(indexes_usage, query_count, support_count)
        indexes_usage.each do |index, statements|
          p index
          statements.each { |s| p s }
          puts
        end

        puts "        Queries: #{query_count.length}"
        puts "Support queries: #{support_count.length}"
      end
    end
  end
end
