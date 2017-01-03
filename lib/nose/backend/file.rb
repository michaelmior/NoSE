# frozen_string_literal: true

module NoSE
  module Backend
    # Simple backend which persists data to a file
    class FileBackend < Backend
      include Subtype

      def initialize(model, indexes, plans, update_plans, config)
        super

        # Try to load data from file or start fresh
        @index_data = if !config[:file].nil? && File.file?(config[:file])
                        Marshal.load File.open(config[:file])
                      else
                        {}
                      end

        # Ensure the data is saved when we exit
        ObjectSpace.define_finalizer self, self.class.finalize(@index_data,
                                                               config[:file])
      end

      # Save data when the object is destroyed
      def self.finalize(index_data, file)
        proc do
          Marshal.dump(index_data, File.open(file, 'w'))
        end
      end

      # Check for an empty array for the data
      def index_empty?(index)
        !index_exists?(index) || @index_data[index.key].empty?
      end

      # Check if we have prepared space for this index
      def index_exists?(index)
        @index_data.key? index.key
      end

      # @abstract Subclasses implement to allow inserting
      def index_insert_chunk(index, chunk)
        @index_data[index.key].concat chunk
      end

      # Generate a simple UUID
      def generate_id
        SecureRandom.uuid
      end

      # Allocate space for data on the new indexes
      def indexes_ddl(execute = false, skip_existing = false,
                      drop_existing = false)
        @indexes.each do |index|
          # Do the appropriate behaviour based on the flags passed in
          if index_exists?(index)
            next if skip_existing
            fail unless drop_existing
          end

          @index_data[index.key] = []
        end if execute

        # We just use the original index definition as DDL
        @indexes.map(&:inspect)
      end

      # Sample a number of values from the given index
      def index_sample(index, count)
        data = @index_data[index.key]
        data.nil? ? [] : data.sample(count)
      end

      # We just produce the data here which can be manipulated as needed
      # @return [Hash]
      def client
        @index_data
      end

      # Provide some helper functions which allow the matching of rows
      # based on a set of list of conditions
      module RowMatcher
        # Check if a row matches the given condition
        # @return [Boolean]
        def row_matches?(row, conditions)
          row_matches_eq?(row, conditions) &&
            row_matches_range?(row, conditions)
        end

        # Check if a row matches the given condition on equality predicates
        # @return [Boolean]
        def row_matches_eq?(row, conditions)
          @eq_fields.all? do |field|
            row[field.id] == conditions.find { |c| c.field == field }.value
          end
        end

        # Check if a row matches the given condition on the range predicate
        # @return [Boolean]
        def row_matches_range?(row, conditions)
          return true if @range_field.nil?

          range_cond = conditions.find { |c| c.field == @range_field }
          row[@range_field.id].send range_cond.operator, range_cond.value
        end
      end

      # Look up data on an index in the backend
      class IndexLookupStatementStep < Backend::IndexLookupStatementStep
        include RowMatcher

        # Filter all the rows in the specified index to those requested
        def process(conditions, results)
          # Get the set of conditions we need to process
          results = initial_results(conditions) if results.nil?
          condition_list = result_conditions conditions, results

          # Loop through all rows to find the matching ones
          rows = @client[@index.key] || []
          selected = condition_list.flat_map do |condition|
            rows.select { |row| row_matches? row, condition }
          end.compact

          # Apply the limit and only return selected fields
          field_ids = Set.new @step.fields.map(&:id).to_set
          selected[0..(@step.limit.nil? ? -1 : @step.limit)].map do |row|
            row.select { |k, _| field_ids.include? k }
          end
        end
      end

      # Insert data into an index on the backend
      class InsertStatementStep < Backend::InsertStatementStep
        # Add new rows to the index
        def process(results)
          key_ids = (@index.hash_fields + @index.order_fields).map(&:id).to_set

          results.each do |row|
            # Pick out primary key fields we can use to match
            conditions = row.select do |field_id|
              key_ids.include? field_id
            end

            # If we have all the primary keys, check for a match
            if conditions.length == key_ids.length
              # Try to find a row with this ID and update it
              matching_row = @client[index.key].find do |index_row|
                index_row.merge(conditions) == index_row
              end

              unless matching_row.nil?
                matching_row.merge! row
                next
              end
            end

            # Populate IDs as needed
            key_ids.each do |key_id|
              row[key_id] = SecureRandom.uuid if row[key_id].nil?
            end

            @client[index.key] << row
          end
        end
      end

      # Delete data from an index on the backend
      class DeleteStatementStep < Backend::DeleteStatementStep
        include RowMatcher

        # Remove rows matching the results from the dataset
        def process(results)
          # Loop over all rows
          @client[index.key].reject! do |row|
            # Check against all results
            results.any? do |result|
              # If all fields match, drop the row
              result.all? do |field, value|
                row[field] == value
              end
            end
          end
        end
      end
    end
  end
end
