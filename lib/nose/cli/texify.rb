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

      option :nesting, type: :numeric, default: 0, aliases: '-n',
                       desc: 'nesting level of LaTeX sections'

      def texify(plan_file)
        # Load the indexes from the file
        result = load_results plan_file, options[:mix]

        # Print document header
        puts "\\documentclass{article}\n\\begin{document}\n"

        # Print the LaTeX for all indexes and plans
        subs = 'sub' * options[:nesting]
        texify_indexes result.indexes, subs
        texify_plans result.plans + result.update_plans, subs

        # End the document
        puts '\\end{document}'
      end

      private

      # Print the LaTeX for all query plans
      def texify_plans(plans, subs)
        puts "\\#{subs}section{Plans}"

        plans.group_by(&:group).each do |group, grouped_plans|
          texify_plan_group group, grouped_plans, subs
        end
      end

      # Print the LaTeX from a group of query plans
      def texify_plan_group(group, grouped_plans, subs)
        puts "\\#{subs}subsection*{#{group}}"

        grouped_plans.each do |plan|
          if plan.is_a? Plans::QueryPlan
            puts texify_plan_steps plan.steps
            puts ' \\\\'
          else
            puts texify_plan_steps plan.query_plans.flat_map(&:to_a) + \
              plan.update_steps
            puts ' \\\\'
          end
        end
      end

      # Print the LaTeX from a set of plan steps
      # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity
      def texify_plan_steps(steps)
        steps.map do |step|
          case step
          when Plans::IndexLookupPlanStep
            "Request \\textbf{#{step.index.key}}"
          when Plans::FilterPlanStep
            "Filter by #{texify_fields((step.eq + [step.range]).compact)}"
          when Plans::SortPlanStep
            "Sort by #{texify_fields step.sort_fields}"
          when Plans::LimitPlanStep
            "Limit #{step.limit}"
          when Plans::DeletePlanStep
            "Delete from \\textbf{#{step.index.key}}"
          when Plans::InsertPlanStep
            "Insert into \\textbf{#{step.index.key}}"
          end
        end.join(', ')
      end
      # rubocop:enable

      # Print all LaTeX for a given index
      def texify_indexes(indexes, subs)
        puts "\\#{subs}section{Indexes}"

        indexes.each do |index|
          # Print the key of the index
          puts "\\#{subs}subsection*{#{index.key}}"

          fields = index.hash_fields.map do |field|
            texify_field(field, true)
          end

          fields += index.order_fields.map do |field|
            texify_field(field, true, true)
          end

          fields += index.extra.map { |field| texify_field(field) }

          puts fields.join(', ')
        end
      end

      # Produce the LaTex for an array of fields
      def texify_fields(fields)
        fields.map { |field| texify_field field }.join ', '
      end

      # Produce the LaTeX for a given index field
      def texify_field(field, underline = false, italic = false)
        tex = "#{field.to_s.gsub '_', '\\_'}"
        tex = "\\textit{#{tex}}" if italic
        tex = "\\underline{#{tex}}" if underline

        tex
      end
    end
  end
end
