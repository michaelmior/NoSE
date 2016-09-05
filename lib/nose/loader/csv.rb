# frozen_string_literal: true

require 'formatador'
require 'smarter_csv'
require 'zlib'

module NoSE
  module Loader
    # Load data into an index from a set of CSV files
    class CsvLoader < LoaderBase
      def initialize(workload = nil, backend = nil)
        super

        @logger = Logging.logger['nose::loader::csvloader']
      end

      # Load data for all the indexes
      def load(indexes, config, show_progress = false, limit = nil,
               skip_existing = true)
        indexes.map!(&:to_id_graph).uniq! if @backend.by_id_graph

        simple_indexes = find_simple_indexes indexes, skip_existing
        simple_indexes.each do |entity, simple_index_list|
          filename = File.join config[:directory], "#{entity.name}.csv"
          total_rows = (limit || 0) - 1 # account for header row
          File.open(filename) { |file| file.each_line { total_rows += 1 } }

          progress = initialize_progress entity, simple_index_list,
                                         total_rows if show_progress
          load_file_indexes filename, entity, simple_index_list, progress
        end
      end

      private

      # Find the simple indexes we should populate
      # @return [Hash<Entity, Index>]
      def find_simple_indexes(indexes, skip_existing)
        simple_indexes = indexes.select do |index|
          index.graph.size == 1 &&
            !(skip_existing && !@backend.index_empty?(index))
        end

        simple_indexes.group_by do |index|
          index.hash_fields.first.parent
        end
      end

      # Initialize a progress bar to reporting loading results
      # @return [Formatador::ProgressBar]
      def initialize_progress(entity, simple_index_list, total_rows)
        @logger.info "Loading simple indexes for #{entity.name}"
        @logger.info simple_index_list.map(&:key).join(', ')

        Formatador.new.redisplay_progressbar 0, total_rows
        Formatador::ProgressBar.new total_rows, started_at: Time.now.utc
      end

      # Load all indexes for a given file
      # @return [void]
      def load_file_indexes(filename, entity, simple_index_list, progress)
        SmarterCSV.process(filename,
                           downcase_header: false,
                           chunk_size: 1000,
                           convert_values_to_numeric: false) do |chunk|
          Parallel.each(chunk.each_slice(100),
                        finish: (lambda do |_, _, _|
                          next if progress.nil?
                          inc = [progress.total - progress.current, 100].min
                          progress.increment inc
                        end)) do |minichunk|
            load_simple_chunk minichunk, entity, simple_index_list
          end
        end
      end

      # Load a chunk of data from a simple entity index
      # @return [void]
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
