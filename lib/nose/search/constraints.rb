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
        constraints.each do |entity, constraint|
          # If this is a support query, then we might not need a plan
          if query.is_a? SupportQuery
            # Find the index associated with the support query and make
            # the requirement of a plan conditional on this index
            index_var = problem.index_vars[query.index]
            problem.model.addConstr constraint == index_var * 1.0,
                                    "q#{q}_#{entity.name}"
          else
            problem.model.addConstr constraint == 1,
                                    "q#{q}_#{entity.name}"
          end
        end
      end

      # Add complete query plan constraints
      def self.apply_query(query, q, problem)
        entities = query.longest_entity_path
        query_constraints = Hash[entities.map do |entity|
          [entity, Gurobi::LinExpr.new]
        end]

        problem.data[:costs][query].each do |index, (step_indexes, _)|
          index.path.entities.each do |entity|
            index_var = problem.query_vars[index][query]
            query_constraints[entity] += index_var
          end

          # No additional work is necessary if we only have one step here
          next if step_indexes.length == 1

          # All indices used at this step must either all be used, or none used
          if step_indexes.last.is_a? Array
            # We have multiple possible last steps, so add an auxiliary
            # variable which allows us to select between them
            step_var = problem.model.addVar 0, 1, 0, Gurobi::BINARY,
                                            "q#{q}_s#{step_indexes.first.key}"
            problem.model.update

            # Force exactly one of the indexes for the last step to be chosen
            last_vars = step_indexes.last.map do |index2|
              problem.query_vars[index2][query]
            end.inject(Gurobi::LinExpr.new, &:+)
            problem.model.addConstr(last_vars == step_var)

            # Replace the collection of last variables by the step variable
            vars = step_indexes[0..-2].map do |index2|
              problem.query_vars[index2][query]
            end + [step_var]
          else
            vars = step_indexes.map do |index2|
              problem.query_vars[index2][query]
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
