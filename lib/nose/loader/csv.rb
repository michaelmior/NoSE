require 'formatador'
require 'smarter_csv'
require 'zlib'

module NoSE
  module Loader
    # Load data into an index from a set of CSV files
    class CsvLoader < LoaderBase
      # Load data for all the indexes
      def load(indexes, config, show_progress = false, limit = nil,
                                skip_existing = true)
        simple_indexes = indexes.select do |index|
          index.path.length == 1 &&
          !(skip_existing && !@backend.index_empty?(index))
        end

        simple_indexes = simple_indexes.group_by do |index|
          index.path.first.parent
        end
        simple_indexes.each do |entity, simple_index_list|
          filename = File.join config[:directory], "#{entity.name}.csv"
          total_rows = (limit || 0) - 1  # account for header row
          File.open(filename) { |file| file.each_line { total_rows += 1 } }

          if show_progress
            puts "Loading simple indexes for #{entity.name}"
            puts "#{simple_index_list.map(&:key).join ', '}"

            Formatador.new.redisplay_progressbar 0, total_rows
            progress = Formatador::ProgressBar.new total_rows,
                                                   started_at: Time.now
          else
            progress = nil
          end

          SmarterCSV.process(filename,
                             downcase_header: false,
                             chunk_size: 1000,
                             convert_values_to_numeric: false) do |chunk|
            Parallel.each(chunk.each_slice(100),
                          finish: (lambda do |_, _, _|
                            next unless show_progress
                            inc = [progress.total - progress.current, 100].min
                            progress.increment inc
                          end)) do |minichunk|
              load_simple_chunk minichunk, entity, simple_index_list
            end
          end
        end
      end

      private

      # Load a chunk of data from a simple entity index
      def load_simple_chunk(chunk, entity, indexes)
        # Prefix all hash keys with the entity name and convert values
        chunk.map! do |row|
          index_row = {}
          row.each_key do |key|
            field_class = entity[key.to_s].class
            value = field_class.value_from_string row[key]
            index_row["#{entity.name}_#{key}"] = value
          end

          index_row
        end

        # Insert the batch into the index
        indexes.each do |index|
          @backend.index_insert_chunk index, chunk
        end
      end
    end
  end
end
