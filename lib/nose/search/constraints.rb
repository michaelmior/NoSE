module NoSE
  # Base class for constraints
  class Constraint
    # If this is not overridden, apply query-specific constraints
    def self.apply(workload, model, index_vars, query_vars, indexes, data)
      workload.queries.each_with_index do |query, q|
        self.apply_query query, q, workload, model, index_vars, query_vars,
                         indexes, data
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
    def self.apply(workload, model, index_vars, query_vars, indexes, data)
      (0...indexes.length).each do |i|
        (0...workload.queries.length).each do |q|
          model.addConstr query_vars[i][q] + index_vars[i] * -1 <= 0,
            "i#{i}q#{q}_avail"
        end
      end
    end
  end

  # The single constraint used to enforce a maximum storage cost
  class SpaceConstraint < Constraint
    # Add space constraint if needed
    def self.apply(workload, model, index_vars, query_vars, indexes, data)
      return unless data[:max_space].finite?

      space = indexes.each_with_index.map do |index, i|
        index_vars[i] * (index.size * 1.0)
      end.reduce(&:+)
      model.addConstr space <= data[:max_space] * 1.0, 'max_space'
    end
  end

  # Constraints that force each query to have an available plan
  class CompletePlanConstraints < Constraint
    private

    # Add complete query plan constraints
    def self.apply_query(query, q, workload, model, index_vars, query_vars,
                         indexes, data)
      entities = query.longest_entity_path
      query_constraint = Array.new(entities.length) { Gurobi::LinExpr.new }

      data[:costs][q].each do |i, (step_indexes, _)|
        indexes[i].entity_range(entities).each do |part|
          index_var = query_vars[i][q]
          query_constraint[part] += index_var
        end

        # All indices used at this step must either all be used, or none used
        if step_indexes.length > 1
          if step_indexes.last.is_a? Array
            # We have multiple possible last steps, so add an auxiliary
            # variable which allows us to select between them
            first_var = query_vars[step_indexes.first][q]
            step_var = model.addVar(0, 1, 0, Gurobi::BINARY,
                                    "q#{q}s#{step_indexes.first}")
            model.update
            model.addConstr(step_var * 1 >= first_var * 1)

            # Force exactly one of the indexes for the last step to be chosen
            vars = step_indexes.last.map { |index| query_vars[index][q] }
            model.addConstr(vars.inject(Gurobi::LinExpr.new, &:+) ==
                            step_var)
          else
            vars = step_indexes.map { |index| query_vars[index][q] }
            vars.reverse.each_cons(2).each do |first_var, last_var|
              model.addConstr(last_var * 1 == first_var * 1)
            end
          end
        end
      end

      # Ensure we have exactly one index on each component of the query path
      query_constraint.each do |constraint|
        model.addConstr(constraint == 1)
      end
    end
  end
end
