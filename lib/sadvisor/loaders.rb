require 'smarter_csv'

module Sadvisor
  # Load data into an index from a set of CSV files
  class CSVLoader
    def initialize(workload, backend)
      @workload = workload
      @backend = backend
    end

    # Load data for all the indexes
    def load(_indexes, directory)
      @workload.entities.each do |name, entity|
        SmarterCSV.process(File.join(directory, "#{name}.csv"),
                           chunk_size: 10,
                           convert_values_to_numeric: false).each do |chunk|
          load_simple_chunk chunk, entity
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
