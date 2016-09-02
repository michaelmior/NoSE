# frozen_string_literal: true

require 'csv'
require 'gruff'

module NoSE
  module CLI
    # Add a command to generate a graphic of the schema from a workload
    class NoSECLI < Thor
      desc 'analyze OUTPUT_FILE CSV_FILES',
           'output a graph to OUTPUT_FILE comparing the CSV_FILES'

      long_desc <<-LONGDESC
        `nose analyze` will create a graph comparing the runtimes from multiple
        runs of different schemas for a particular workload. The CSV files
        should be of the format produced from either `nose benchmark` or
        `nose execute`.
      LONGDESC

      option :total, type: :boolean, default: false, aliases: '-t',
                     desc: 'whether to include a line for totals in the '\
                           'graph'

      def analyze(output_file, *csv_files)
        # Load data from the files
        data = load_data csv_files, options[:total]

        # Set graph properties
        g = Gruff::Bar.new '2000x800'
        g.title = 'NoSE Schema Performance'
        g.x_axis_label = '\nWorkload group'
        g.y_axis_label = 'Weighted execution time (s)'
        g.title_font_size = 20
        g.legend_font_size = 10
        g.marker_font_size = 10
        g.label_stagger_height = 15
        g.legend_box_size = 10
        g.bar_spacing = 0.5

        # Add each data element to the graph
        data.each do |datum|
          g.data datum.first['label'], datum.map { |row| row['mean'] }
        end
        g.labels = Hash[data.first.map.with_index do |row, n|
          [n, row['group']]
        end]

        g.write output_file
      end

      private

      # Load the data from the given list of CSV files
      # @return [Array<Hash>]
      def load_data(csv_files, total = false)
        headers = nil
        csv_files.map do |file|
          lines = CSV.read(file)
          headers = lines.first if headers.nil?

          lines = lines[1..-1].map do |row|
            Hash[headers.zip row]
          end
          grouped_lines = lines.group_by { |row| row['group'] }

          rows = grouped_lines.map do |group, grouped_rows|
            mean = grouped_rows.inject(0) { |sum, row| sum + row['mean'].to_f }
            {
              'label' => grouped_rows.first['label'],
              'group' => group,
              'weight' => grouped_rows.first['weight'].to_f,
              'mean' => mean
            }
          end

          # Add an additional row for the total
          if total
            total_weight = rows.map { |row| row['weight'] }.inject(0, &:+)
            rows << {
              'label' => rows.first['label'],
              'group' => 'TOTAL',
              'weight' => 1.0,
              'mean' => rows.inject(0) do |sum, row|
                sum + row['mean'] * row['weight'] / total_weight
              end
            }
          end

          rows
        end
      end
    end
  end
end
