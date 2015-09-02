module NoSE
  module CLI
    # Add a command to reformat a plan file
    class NoSECLI < Thor
      desc 'reformat PLAN_FILE',
           'reformat the data in PLAN_FILE'
      option :mix, type: :string, default: 'default',
                   banner: 'the name of the workload mix for weighting queries'
      option :format, type: :string, default: 'txt',
                      enum: %w(txt json yml), aliases: '-f'
      def reformat(plan_file)
        result = load_results plan_file
        result.workload.mix = options[:mix].to_sym \
          unless options[:mix] == 'default' && result.workload.mix != :default
        result.recalculate_cost

        # Output the results in the specified format
        send(('output_' + options[:format]).to_sym, result)
      end
    end
  end
end
