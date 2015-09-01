module NoSE
  module CLI
    # Add a command to check for possible data integrity violations
    class NoSECLI < Thor
      desc 'check-integrity PLANS',
           'find possible integrity violations in the named PLANS'
      long_desc <<-LONGDESC
        `nose check-integrity` will load a set of manually defined query
        execution plans and check for potential integrity violations.
        It simply examines inserts into indexes and looks for other indexes
        which contain the same attributes but are not modified in that group.

        NOTE: This currently only deals with inserts and might not be
              completely accurate. Expect both false positives and false
              negatives.
      LONGDESC
      def check_integrity(plans_name)
        # Load the execution plans
        plans = Plans::ExecutionPlans.load plans_name
        indexes = plans.schema.indexes

        valid = true
        plans.groups.each do |group, plans|
          # Find plans which modify data
          update_steps = plans.flat_map(&:update_steps)
          next if update_steps.empty?

          # Find the fields and indexes which were updated
          updated_fields = update_steps.inject(Set.new) do |fields, step|
            fields.merge(step.fields.to_set - step.index.hash_fields \
                                            - step.index.order_fields.to_set)
          end
          updated_indexes = update_steps.map(&:index).to_set

          # Find the indexes which should be updated
          should_update = indexes.each_value.select do |index|
            !index.all_fields.disjoint? updated_fields
          end.to_set

          # Check that all the indexes were correctly updated
          didnt_update = should_update - updated_indexes
          next if didnt_update.empty?

          valid = false
          didnt_update.each do |index|
            puts Formatador.parse "[red]Index[/] [magenta]#{index.key}[/]" \
                                  " [red]may be missing update in[/] #{group}!"
          end
        end

        # Fail if an invalid index was discovered
        exit 1 unless valid
      end
    end
  end
end
