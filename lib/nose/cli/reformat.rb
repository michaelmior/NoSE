module NoSE
  module CLI
    # Add a command to reformat a plan file
    class NoSECLI < Thor
      desc 'reformat PLAN_FILE',
           'reformat the data in PLAN_FILE'
      option :format, type: :string, default: 'txt',
                      enum: %w(txt json yml), aliases: '-f'
      def reformat(plan_file)
        result = load_results plan_file

        # Output the results in the specified format
        send(('output_' + options[:format]).to_sym, result)
      end
    end
  end
end
