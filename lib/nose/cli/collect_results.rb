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
                     banner: 'whether to include a line for totals in the '\
                             'graph'
      def collect_results(*csv_files)
        # Load the data and output the header
        data = load_data csv_files, options[:total]
        labels = data.map { |datum| datum.first['label'] }
        puts((['Group'] + labels).join "\t")

        # Output the mean for each schema
        1.upto(data.first.count) do |n|
          print data.first[n - 1]['group'] + "\t"
          puts(data.map { |datum| datum[n - 1]['mean'] }.join "\t")
        end
      end
    end
  end
end
