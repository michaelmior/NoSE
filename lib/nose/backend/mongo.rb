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
            [index.graph.longest_path.path_for_field(key).join('.'), 1]
          end]

          ddl << "Add index #{index_spec} to #{id_graph.key} (#{index.key})"
          next unless execute

          client[id_graph.key].indexes.create_one index_spec
        end

        ddl
      end

      private

      # Create a Mongo client from the saved config
      def client
        @client ||= Mongo::Client.new @uri, database: @database
      end
    end
  end
end
