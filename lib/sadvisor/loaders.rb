require 'formatador'
require 'smarter_csv'

module Sadvisor
  # Load data into an index from a set of CSV files
  class CSVLoader
    def initialize(workload, backend)
      @workload = workload
      @backend = backend
    end

    # Load data for all the indexes
    def load(_indexes, directory, show_progress = false)
      @workload.entities.each do |name, entity|
        filename = File.join directory, "#{name}.csv"
        total_rows = -1  # account for header row
        File.foreach(filename) { total_rows += 1 }

        puts "Loading #{entity.name}" if show_progress
        if show_progress
          Formatador.new.redisplay_progressbar 0, total_rows
          progress = Formatador::ProgressBar.new total_rows,
                                                 started_at: Time.now
        else
          progress = nil
        end

        SmarterCSV.process(filename,
                           chunk_size: 1000,
                           convert_values_to_numeric: false) do |chunk|
          Parallel.each(chunk.each_slice(100),
                        finish: (lambda do |_, _, _|
                          inc = [progress.total - progress.current, 100].min
                          progress.increment inc if progress
                        end)) do |minichunk|
            load_simple_chunk minichunk, entity
          end
        end
      end
    end

    private

    # Load a chunk of data from a simple entity index
    def load_simple_chunk(chunk, entity)

      # Prefix all hash keys with the entity name and convert values
      chunk.map! do |row|
        index_row = {}
        row.keys.each do |key|
          field_class = entity[key.to_s].class
          value = field_class.value_from_string row[key]
          value = Cql::Uuid.new value.hash \
            if field_class.ancestors.include? IDField
          index_row["#{entity.name}_#{key}"] = value
        end

        index_row
      end

      # Insert the batch into the index
      @backend.index_insert_chunk entity.simple_index, chunk
    end
  end
end
