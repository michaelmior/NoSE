# frozen_string_literal: true

module NoSE
  module CLI
    # Add a command to reformat a plan file
    class NoSECLI < Thor
      desc 'reformat PLAN_FILE',
           'reformat the data in PLAN_FILE'

      shared_option :format
      shared_option :mix

      def reformat(plan_file)
        result = load_results plan_file, options[:mix]
        result.recalculate_cost

        # Output the results in the specified format
        send(('output_' + options[:format]).to_sym, result)
      end
    end
  end
end
