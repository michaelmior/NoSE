module NoSE
  module Search
    # Base class for constraints
    class Constraint
      # If this is not overridden, apply query-specific constraints
      def self.apply(problem)
        problem.queries.each_with_index do |query, q|
          self.apply_query query, q, problem
        end
      end

      private

      # To be implemented in subclasses for query-specific constraints
      def self.apply_query(*args)
      end
    end

    # Constraints which force indexes to be present to be used
    class IndexPresenceConstraints < Constraint
      # Add constraint for indices being present
      def self.apply(problem)
        problem.indexes.each do |index|
          problem.queries.each_with_index do |query, q|
            problem.model.addConstr problem.query_vars[index][query] +
                                    problem.index_vars[index] * -1 <= 0,
                                    "q#{q}_#{index.key}_avail"
          end
        end
      end
    end

    # The single constraint used to enforce a maximum storage cost
    class SpaceConstraint < Constraint
      # Add space constraint if needed
      def self.apply(problem)
        return unless problem.data[:max_space].finite?

        space = problem.indexes.map do |index|
          problem.index_vars[index] * (index.size * 1.0)
        end.reduce(&:+)
        problem.model.addConstr space <= problem.data[:max_space] * 1.0,
                                'max_space'
      end
    end

    # Constraints that force each query to have an available plan
    class CompletePlanConstraints < Constraint
      private

      # Add the discovered constraints to the problem
      def self.add_query_constraints(query, q, constraints, problem)
        constraints.each do |entities, constraint|
          name = "q#{q}_#{entities.map(&:name).join '_'}"

          # If this is a support query, then we might not need a plan
          if query.is_a? SupportQuery
            # Find the index associated with the support query and make
            # the requirement of a plan conditional on this index
            index_var = problem.index_vars[query.index]
            problem.model.addConstr constraint == index_var * 1.0, name
          else
            problem.model.addConstr constraint == 1, name
          end
        end
      end

      # Add complete query plan constraints
      def self.apply_query(query, q, problem)
        entities = query.longest_entity_path.reverse
        query_constraints = Hash[entities.each_cons(2).map do |e, next_e|
          [[e, next_e], Gurobi::LinExpr.new]
        end]

        # Add the sentinel entity at the end
        last = Entity.new '__LAST__'
        query_constraints[[entities.last, last]] = Gurobi::LinExpr.new

        problem.data[:costs][query].each do |index, (index_steps, _)|
          # All indexes should advance a step if possible
          fail if entities.length > 1 && index.path.length == 1

          # Join each step along the entity path
          index_var = problem.query_vars[index][query]
          index.path.entities.each_cons(2) do |entity, next_entity|
            query_constraints[[entity, next_entity]] += index_var
          end

          # If this query has been answered, add the jump to the last step
          if index_steps.last.is_a?(Array) ? index_steps.last.any? { |step| step.state.answered? } : index_steps.last.state.answered?
            query_constraints[[index.path.entities.last, last]] += index_var
          end

          # No additional work is necessary if we only have one step here
          next if index_steps.length == 1

          # All indices used at this step must either all be used, or none used
          if index_steps.last.is_a? Array
            # We have multiple possible last steps, so add an auxiliary
            # variable which allows us to select between them
            step_var = problem.model.addVar 0, 1, 0, Gurobi::BINARY,
                                            "q#{q}_s#{index_steps.first.index.key}"
            problem.model.update

            # Force exactly one of the indexes for the last step to be chosen
            last_vars = index_steps.last.map do |step2|
              problem.query_vars[step2.index][query]
            end.inject(Gurobi::LinExpr.new, &:+)
            problem.model.addConstr(last_vars == step_var)

            # Replace the collection of last variables by the step variable
            vars = index_steps[0..-2].map do |step2|
              problem.query_vars[step2.index][query]
            end + [step_var]
          else
            vars = index_steps.map do |step2|
              problem.query_vars[step2.index][query]
            end
          end

          # Chain all steps together so we have a complete plan
          vars.reverse.each_cons(2).each do |prev_var, next_var|
            problem.model.addConstr(next_var * 1 == prev_var * 1)
          end
        end

        # Ensure we have exactly one index on each component of the query path
        add_query_constraints query, q, query_constraints, problem
      end
    end
  end
end
