# frozen_string_literal: true

require 'mongo'

module NoSE
  module Backend
    # A backend which communicates with MongoDB
    class MongoBackend < BackendBase
      def initialize(model, indexes, plans, update_plans, config)
        super

        @uri = config[:uri]
        @database = config[:database]
      end

      # Create new MongoDB collections for each index
      def indexes_ddl(execute = false, skip_existing = false,
                      drop_existing = false)
        ddl = []

        # Create the ID graphs for all indexes
        id_graphs = @indexes.map(&:to_id_graph).uniq
        id_graphs.map do |id_graph|
          ddl << "Create #{id_graph.key}"
          next unless execute

          collection = client.collections.find { |c| c.name == id_graph.key }
          collection.drop if drop_existing && !collection.nil?
          client[id_graph.key].create unless skip_existing
        end

        # Create any necessary indexes on the ID graphs
        index_keys = []
        @indexes.sort_by do |index|
          -(index.hash_fields.to_a + index.order_fields).length
        end.each do |index|
          # Check if we already have a prefix of this index created
          keys = index.hash_fields.to_a + index.order_fields
          next if index_keys.any? { |i| i[keys.length - 1] == keys }
          index_keys << keys

          id_graph = index.to_id_graph
          next if id_graph == index

          # Combine the key paths for all fields to create a compound index
          index_spec = Hash[keys.map do |key|
            [field_path(index, key).join('.'), 1]
          end]

          ddl << "Add index #{index_spec} to #{id_graph.key} (#{index.key})"
          next unless execute

          client[id_graph.key].indexes.create_one index_spec
        end

        ddl
      end

      # Insert a chunk of rows into an index
      def index_insert_chunk(index, chunk)
        # We only need to insert into indexes which are ID graphs
        fail unless index == index.to_id_graph

        chunk.map! do |row|
          row_hash = Hash.new { |h, k| h[k] = Hash.new(&h.default_proc) }
          index.all_fields.each do |field|
            field_path = field_path(index, field)
            entity_hash = field_path[0..-2].reduce(row_hash) { |h, k| h[k] }

            if field_path.last == '_id'
              entity_hash[field_path.last] = BSON::ObjectId.new
            else
              entity_hash[field_path.last] = row[field.id]
            end
          end

          row_hash
        end
        client[index.key].insert_many chunk
      end

      private

      # Find the path to a given field
      # @return [Array<String>]
      def field_path(index, field)
        # Find the path from the hash entity to the given key
        field_path = index.graph.path_between index.hash_fields.first.parent,
                                              field.parent
        field_path = field_path.path_for_field(field)

        # Use _id for any primary keys
        field_path[-1] = '_id' if field.is_a? Fields::IDField

        field_path
      end

      # Create a Mongo client from the saved config
      def client
        @client ||= Mongo::Client.new @uri, database: @database
      end
    end
  end
end
