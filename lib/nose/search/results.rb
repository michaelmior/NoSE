# frozen_string_literal: true

module NoSE
  module Search
    # A container for results from a schema search
    class Results
      attr_reader :cost_model
      attr_accessor :enumerated_indexes, :indexes, :total_size, :total_cost,
                    :workload, :update_plans, :plans,
                    :revision, :time, :command, :by_id_graph

      def initialize(problem = nil, by_id_graph = false)
        @problem = problem
        return if problem.nil?
        @by_id_graph = by_id_graph

        # Find the indexes the ILP says the query should use
        @query_indexes = Hash.new { |h, k| h[k] = Set.new }
        @problem.query_vars.each do |index, query_vars|
          query_vars.each do |query, var|
            next unless var.value
            @query_indexes[query].add index
          end
        end
      end

      # Provide access to the underlying model in the workload
      # @return [Model]
      def model
        @workload.nil? ? @model : @workload.model
      end

      # Assign the model to the workload if it exists, otherwise store it
      # @return [void]
      def model=(model)
        if @workload.nil?
          @model = model
        else
          @workload.instance_variable_set :@model, model
        end
      end

      # After setting the cost model, recalculate the cost
      # @return [void]
      def cost_model=(new_cost_model)
        recalculate_cost new_cost_model
        @cost_model = new_cost_model
      end

      # After setting the cost model, recalculate the cost
      # @return [void]
      def recalculate_cost(new_cost_model = nil)
        new_cost_model = @cost_model if new_cost_model.nil?

        (@plans || []).each do |plan|
          plan.each { |s| s.calculate_cost new_cost_model }
        end
        (@update_plans || []).each do |plan|
          plan.update_steps.each { |s| s.calculate_cost new_cost_model }
          plan.query_plans.each do |query_plan|
            query_plan.each { |s| s.calculate_cost new_cost_model }
          end
        end

        # Recalculate the total
        query_cost = (@plans || []).sum_by do |plan|
          plan.cost * @workload.statement_weights[plan.query]
        end
        update_cost = (@update_plans || []).sum_by do |plan|
          plan.cost * @workload.statement_weights[plan.statement]
        end
        @total_cost = query_cost + update_cost
      end

      # Validate that the results of the search are consistent
      # @return [void]
      def validate
        validate_indexes
        validate_query_indexes @plans
        validate_update_indexes

        planned_queries = plans.map(&:query).to_set
        fail InvalidResultsException unless \
          (@workload.queries.to_set - planned_queries).empty?
        validate_query_plans @plans

        validate_update_plans
        validate_objective

        freeze
      end

      # Set the query plans which should be used based on the entire tree
      # @return [void]
      def plans_from_trees(trees)
        @plans = trees.map do |tree|
          # Exclude support queries since they will be in update plans
          query = tree.query
          next if query.is_a?(SupportQuery)

          select_plan tree
        end.compact
      end

      # Select the single query plan from a tree of plans
      # @return [Plans::QueryPlan]
      # @raise [InvalidResultsException]
      def select_plan(tree)
        query = tree.query
        plan = tree.find do |tree_plan|
          tree_plan.indexes.to_set == @query_indexes[query]
        end
        plan.instance_variable_set :@workload, @workload

        fail InvalidResultsException if plan.nil?
        plan
      end

      private

      # Check that the indexes selected were actually enumerated
      # @return [void]
      def validate_indexes
        # We may not have enumerated ID graphs
        check_indexes = @indexes.dup
        @indexes.each do |index|
          check_indexes.delete index.to_id_graph
        end if @by_id_graph

        fail InvalidResultsException unless \
          (check_indexes - @enumerated_indexes).empty?
      end

      # Ensure we only have necessary update plans which use available indexes
      # @return [void]
      def validate_update_indexes
        @update_plans.each do |plan|
          validate_query_indexes plan.query_plans
          valid_plan = @indexes.include?(plan.index)
          fail InvalidResultsException unless valid_plan
        end
      end

      # Check that the objective function has the expected value
      # @return [void]
      def validate_objective
        if @problem.objective_type == Objective::COST
          query_cost = @plans.reduce 0 do |sum, plan|
            sum + @workload.statement_weights[plan.query] * plan.cost
          end
          update_cost = @update_plans.reduce 0 do |sum, plan|
            sum + @workload.statement_weights[plan.statement] * plan.cost
          end
          cost = query_cost + update_cost

          fail InvalidResultsException unless (cost - @total_cost).abs < 0.001
        elsif @problem.objective_type == Objective::SPACE
          size = @indexes.sum_by(&:size)
          fail InvalidResultsException unless (size - @total_size).abs < 0.001
        end
      end

      # Ensure that all the query plans use valid indexes
      # @return [void]
      def validate_query_indexes(plans)
        plans.each do |plan|
          plan.each do |step|
            valid_plan = !step.is_a?(Plans::IndexLookupPlanStep) ||
                         @indexes.include?(step.index)
            fail InvalidResultsException unless valid_plan
          end
        end
      end

      # Validate the query plans from the original workload
      # @return [void]
      def validate_query_plans(plans)
        # Check that these indexes are actually used by the query
        plans.each do |plan|
          fail InvalidResultsException unless \
            plan.indexes.to_set == @query_indexes[plan.query]
        end
      end

      # Validate the support query plans for each update
      # @return [void]
      def validate_update_plans
        @update_plans.each do |plan|
          plan.instance_variable_set :@workload, @workload

          validate_query_plans plan.query_plans
        end
      end
    end

    # Thrown when a search produces invalid results
    class InvalidResultsException < StandardError
    end
  end
end
