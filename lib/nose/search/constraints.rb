module NoSE::Search
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
      (0...problem.indexes.length).each do |i|
        (0...problem.queries.length).each do |q|
          problem.model.addConstr problem.query_vars[i][q] +
                                  problem.index_vars[i] * -1 <= 0,
                                  "i#{i}q#{q}_avail"
        end
      end
    end
  end

  # The single constraint used to enforce a maximum storage cost
  class SpaceConstraint < Constraint
    # Add space constraint if needed
    def self.apply(problem)
      return unless problem.data[:max_space].finite?

      space = problem.indexes.each_with_index.map do |index, i|
        problem.index_vars[i] * (index.size * 1.0)
      end.reduce(&:+)
      problem.model.addConstr space <= problem.data[:max_space] * 1.0,
                              'max_space'
    end
  end

  # Constraints that force each query to have an available plan
  class CompletePlanConstraints < Constraint
    private

    # Add the discovered constraints to the problem
    def self.add_query_constraints(query, constraints, problem)
      constraints.each do |constraint|
        # If this is a support query, then we might not need a plan
        if query.is_a? NoSE::SupportQuery
          # Find the index associated with the support query and make
          # the requirement of a plan conditional on this index
          index_var = problem.index_vars[problem.indexes.index(query.index)]
          problem.model.addConstr(constraint == index_var * 1.0)
        else
          problem.model.addConstr(constraint == 1)
        end
      end
    end

    # Add complete query plan constraints
    def self.apply_query(query, q, problem)
      entities = query.longest_entity_path
      query_constraints = Array.new(entities.length) { Gurobi::LinExpr.new }

      problem.data[:costs][q].each do |i, (step_indexes, _)|
        problem.indexes[i].entity_range(entities).each do |part|
          index_var = problem.query_vars[i][q]
          query_constraints[part] += index_var
        end

        # All indices used at this step must either all be used, or none used
        if step_indexes.length > 1
          if step_indexes.last.is_a? Array
            # We have multiple possible last steps, so add an auxiliary
            # variable which allows us to select between them
            first_var = problem.query_vars[step_indexes.first][q]
            step_var = problem.model.addVar 0, 1, 0, Gurobi::BINARY,
                                            "q#{q}s#{step_indexes.first}"
            problem.model.update
            problem.model.addConstr(step_var * 1 >= first_var * 1)

            # Force exactly one of the indexes for the last step to be chosen
            vars = step_indexes.last.map do |index|
              problem.query_vars[index][q]
            end
            problem.model.addConstr(vars.inject(Gurobi::LinExpr.new, &:+) ==
                                                step_var)
          else
            vars = step_indexes.map { |index| problem.query_vars[index][q] }
            vars.reverse.each_cons(2).each do |first_var, last_var|
              problem.model.addConstr(last_var * 1 == first_var * 1)
            end
          end
        end
      end

      # Ensure we have exactly one index on each component of the query path
      add_query_constraints query, query_constraints, problem
    end
  end
end
