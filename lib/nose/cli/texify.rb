# frozen_string_literal: true

module NoSE
  module CLI
    # Add a command to reformat a plan file
    class NoSECLI < Thor
      desc 'texify PLAN_FILE',
           'print the results from PLAN_FILE in LaTeX format'

      long_desc <<-LONGDESC
        `nose texify` loads the generated schema from the given file and prints
        the generated schema in LaTeX format.
      LONGDESC

      shared_option :mix

      def texify(plan_file)
        # Load the indexes from the file
        result, = load_plans plan_file, options

        # If these are manually generated plans, load them separately
        if result.plans.nil?
          plans = Plans::ExecutionPlans.load(plan_file) \
                  .groups.values.flatten(1)
          result.plans = plans.select { |p| p.update_steps.empty? }
          result.update_plans = plans.reject { |p| p.update_steps.empty? }
        end

        # Print document header
        puts "\\documentclass{article}\n\\begin{document}\n\\begin{flushleft}"

        # Print the LaTeX for all indexes and plans
        texify_indexes result.indexes
        texify_plans result.plans + result.update_plans

        # End the document
        puts "\\end{flushleft}\n\\end{document}"
      end

      private

      # Escape values for latex output
      # @return [String]
      def tex_escape(str)
        str.gsub '_', '\\_'
      end

      # Print the LaTeX for all query plans
      # @return [void]
      def texify_plans(plans)
        puts '\\bigskip\\textbf{Plans} \\\\\\bigskip'

        plans.group_by(&:group).each do |group, grouped_plans|
          group = group.nil? ? '' : tex_escape(group)
          texify_plan_group group, grouped_plans
        end
      end

      # Print the LaTeX from a group of query plans
      # @return [void]
      def texify_plan_group(group, grouped_plans)
        puts "\\textbf{#{group}} \\\\" unless group.empty?

        grouped_plans.each do |plan|
          if plan.is_a?(Plans::QueryPlan) ||
             (plan.is_a?(Plans::QueryExecutionPlan) &&
              plan.update_steps.empty?)
            puts texify_plan_steps plan.steps
          else
            puts texify_plan_steps plan.query_plans.flat_map(&:to_a) + \
                                   plan.update_steps
          end

          puts ' \\\\'
        end

        puts '\\medskip'
      end

      # Print the LaTeX from a set of plan steps
      # @return [void]
      def texify_plan_steps(steps)
        steps.map do |step|
          case step
          when Plans::IndexLookupPlanStep
            "Request \\textbf{#{tex_escape step.index.key}}"
          when Plans::FilterPlanStep
            "Filter by #{texify_fields((step.eq + [step.range]).compact)}"
          when Plans::SortPlanStep
            "Sort by #{texify_fields step.sort_fields}"
          when Plans::LimitPlanStep
            "Limit #{step.limit}"
          when Plans::DeletePlanStep
            "Delete from \\textbf{#{tex_escape step.index.key}}"
          when Plans::InsertPlanStep
            "Insert into \\textbf{#{tex_escape step.index.key}}"
          end
        end.join(', ')
      end

      # Print all LaTeX for a given index
      # @return [void]
      def texify_indexes(indexes)
        puts '\\bigskip\\textbf{Indexes} \\\\\\bigskip'

        indexes.each do |index|
          # Print the key of the index
          puts "\\textbf{#{tex_escape index.key}} \\\\"

          fields = index.hash_fields.map do |field|
            texify_field(field, true)
          end

          fields += index.order_fields.map do |field|
            texify_field(field, true, true)
          end

          fields += index.extra.map { |field| texify_field(field) }

          puts fields.join(', ') + ' \\\\\\medskip'
        end
      end

      # Produce the LaTex for an array of fields
      # @return [String]
      def texify_fields(fields)
        fields.map { |field| texify_field field }.join ', '
      end

      # Produce the LaTeX for a given index field
      # @return [String]
      def texify_field(field, underline = false, italic = false)
        tex = tex_escape field.to_s
        tex = "\\textit{#{tex}}" if italic
        tex = "\\underline{#{tex}}" if underline

        tex
      end
    end
  end
end
