# frozen_string_literal: true

module NoSE
  module CLI
    # Add a command to generate a graphic of the schema from a workload
    class NoSECLI < Thor
      desc 'collect-results CSV_FILES',
           'collect results from CSV_FILES and produce a `gnuplot` data file'

      long_desc <<-LONGDESC
        `nose collect-results` combines results from multiple statement
        execution runs and produces a data file which can be used to generate
        clustered bar charts in `gnuplot`.
      LONGDESC

      option :total, type: :boolean, default: false, aliases: '-t',
                     desc: 'whether to include a line for totals in the '\
                           'graph'

      def collect_results(*csv_files)
        # Load the data and output the header
        data = load_data csv_files, options[:total]
        labels = data.map { |datum| datum.first['label'] }
        puts((['Group'] + labels).join("\t"))

        # Output the mean for each schema
        group_data(data).each { |group| collect_group_data group, data }
      end

      private

      # Combine the results into groups
      # @return [Array]
      def group_data(data)
        # Make sure we collect all rows, keeping the total last
        groups = data.map { |d| d.map { |r| r['group'] } }.flatten.uniq
        groups.delete 'TOTAL'
        groups << 'TOTAL' if options[:total]

        groups
      end

      # Collect the results for a single group
      # @return [void]
      def collect_group_data(group, data)
        print group + "\t"
        data.each do |datum|
          row = datum.find { |r| r['group'] == group }
          print((row.nil? ? '' : row['mean'].to_s) + "\t")
        end
        puts
      end
    end
  end
end
