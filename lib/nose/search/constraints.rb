# frozen_string_literal: true

module NoSE
  module Search
    # Base class for constraints
    class Constraint
      # If this is not overridden, apply query-specific constraints
      # @return [void]
      def self.apply(problem)
        problem.queries.each_with_index do |query, q|
          apply_query query, q, problem
        end
      end

      # To be implemented in subclasses for query-specific constraints
      # @return [void]
      def self.apply_query(*_args)
      end
    end

    # Constraints which force indexes to be present to be used
    class IndexPresenceConstraints < Constraint
      # Add constraint for indices being present
      def self.apply(problem)
        problem.indexes.each do |index|
          problem.queries.each_with_index do |query, q|
            name = "q#{q}_#{index.key}_avail" if ENV['NOSE_LOG'] == 'debug'
            constr = MIPPeR::Constraint.new problem.query_vars[index][query] +
                                            problem.index_vars[index] * -1,
                                            :<=, 0, name
            problem.model << constr
          end
        end
      end
    end

    # The single constraint used to enforce a maximum storage cost
    class SpaceConstraint < Constraint
      # Add space constraint if needed
      def self.apply(problem)
        return unless problem.data[:max_space].finite?

        fail 'Space constraint not supported when grouping by ID graph' \
          if problem.data[:by_id_graph]

        space = problem.total_size
        constr = MIPPeR::Constraint.new space, :<=,
                                        problem.data[:max_space] * 1.0,
                                        'max_space'
        problem.model << constr
      end
    end

    # Constraints that force each query to have an available plan
    class CompletePlanConstraints < Constraint
      # Add the discovered constraints to the problem
      def self.add_query_constraints(query, q, constraints, problem)
        constraints.each do |entities, constraint|
          name = "q#{q}_#{entities.map(&:name).join '_'}" \
              if ENV['NOSE_LOG'] == 'debug'

          # If this is a support query, then we might not need a plan
          if query.is_a? SupportQuery
            # Find the index associated with the support query and make
            # the requirement of a plan conditional on this index
            index_var = if problem.data[:by_id_graph]
                          problem.index_vars[query.index.to_id_graph]
                        else
                          problem.index_vars[query.index]
                        end
            next if index_var.nil?

            constr = MIPPeR::Constraint.new constraint + index_var * -1.0,
                                            :==, 0, name
          else
            constr = MIPPeR::Constraint.new constraint, :==, 1, name
          end

          problem.model << constr
        end
      end

      # Add complete query plan constraints
      def self.apply_query(query, q, problem)
        entities = query.join_order
        query_constraints = Hash[entities.each_cons(2).map do |e, next_e|
          [[e, next_e], MIPPeR::LinExpr.new]
        end]

        # Add the sentinel entities at the end and beginning
        last = Entity.new '__LAST__'
        query_constraints[[entities.last, last]] = MIPPeR::LinExpr.new
        first = Entity.new '__FIRST__'
        query_constraints[[entities.first, first]] = MIPPeR::LinExpr.new

        problem.data[:costs][query].each do |index, (steps, _)|
          # All indexes should advance a step if possible unless
          # this is either the last step from IDs to entity
          # data or the first step going from data to IDs
          index_step = steps.first
          fail if entities.length > 1 && index.graph.size == 1 && \
                  !(steps.last.state.answered? ||
                    index_step.parent.is_a?(Plans::RootPlanStep))

          # Join each step in the query graph
          index_var = problem.query_vars[index][query]
          index_entities = index.graph.entities.sort_by do |entity|
            entities.index entity
          end
          index_entities.each_cons(2) do |entity, next_entity|
            # Make sure the constraints go in the correct direction
            if query_constraints.key?([entity, next_entity])
              query_constraints[[entity, next_entity]] += index_var
            else
              query_constraints[[next_entity, entity]] += index_var
            end
          end

          # If this query has been answered, add the jump to the last step
          query_constraints[[entities.last, last]] += index_var \
            if steps.last.state.answered?

          # If this index is the first step, add this index to the beginning
          query_constraints[[entities.first, first]] += index_var \
            if index_step.parent.is_a?(Plans::RootPlanStep)

          # Ensure the previous index is available
          parent_index = index_step.parent.parent_index
          next if parent_index.nil?

          parent_var = problem.query_vars[parent_index][query]
          name = "q#{q}_#{index.key}_parent" if ENV['NOSE_LOG'] == 'debug'
          constr = MIPPeR::Constraint.new index_var * 1.0 + parent_var * -1.0,
                                          :<=, 0, name
          problem.model << constr
        end

        # Ensure we have exactly one index on each component of the query graph
        add_query_constraints query, q, query_constraints, problem
      end
    end
  end
end
