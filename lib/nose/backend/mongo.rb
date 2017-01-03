# frozen_string_literal: true

require 'mongo'

module NoSE
  module Backend
    # A backend which communicates with MongoDB
    class MongoBackend < Backend
      include Subtype

      def initialize(model, indexes, plans, update_plans, config)
        super

        @uri = config[:uri]
        @database = config[:database]
        Mongo::Logger.logger.level = ::Logger::FATAL
      end

      # MongoDB uses ID graphs for column families
      # @return [Boolean]
      def by_id_graph
        true
      end

      # Produce a new ObjectId
      # @return [BSON::ObjectId]
      def generate_id
        BSON::ObjectId.new
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
            [self.class.field_path(index, key).join('.'), 1]
          end]

          ddl << "Add index #{index_spec} to #{id_graph.key} (#{index.key})"
          next unless execute

          client[id_graph.key].indexes.create_one index_spec
        end

        ddl
      end

      # Insert a chunk of rows into an index
      # @return [Array<BSON::ObjectId>]
      def index_insert_chunk(index, chunk)
        # We only need to insert into indexes which are ID graphs
        fail unless index == index.to_id_graph

        chunk.map! do |row|
          row_hash = Hash.new { |h, k| h[k] = Hash.new(&h.default_proc) }
          index.all_fields.each do |field|
            field_path = self.class.field_path(index, field)
            entity_hash = field_path[0..-2].reduce(row_hash) { |h, k| h[k] }

            if field_path.last == '_id'
              entity_hash[field_path.last] = BSON::ObjectId.new
            else
              entity_hash[field_path.last] = row[field.id]
            end
          end

          row_hash.default_proc = nil
          row_hash
        end

        client[index.key].insert_many(chunk, ordered: false).inserted_ids
      end

      # Sample a number of values from the given index
      def index_sample(index, count)
        rows = client[index.to_id_graph.key].aggregate(
          [
            { '$sample' => { 'size' => count } }
          ]
        ).to_a

        MongoBackend.rows_from_mongo rows, index
      end

      # Convert documens returned from MongoDB into the format we understand
      # @return [Array<Hash>]
      def self.rows_from_mongo(rows, index, fields = nil)
        fields = index.all_fields if fields.nil?

        rows.map! do |row|
          Hash[fields.map do |field|
            field_path = MongoBackend.field_path(index, field)
            [field.id, field_path.reduce(row) { |h, p| h[p] }]
          end]
        end
      end

      # Find the path to a given field
      # @return [Array<String>]
      def self.field_path(index, field)
        # Find the path from the hash entity to the given key
        field_path = index.graph.path_between index.hash_fields.first.parent,
                                              field.parent
        field_path = field_path.path_for_field(field)

        # Use _id for any primary keys
        field_path[-1] = '_id' if field.is_a? Fields::IDField

        field_path
      end

      # Insert data into an index on the backend
      class InsertStatementStep < Backend::InsertStatementStep
        def initialize(client, index, fields)
          super

          @fields = fields.map(&:id) & index.all_fields.map(&:id)
        end

        # Insert each row into the index
        def process(results)
          results.each do |result|
            values = Hash[@index.all_fields.map do |field|
              next unless result.key? field.id
              value = result[field.id]

              # If this is an ID, generate or construct an ObjectId
              if field.is_a?(Fields::IDField)
                value = if value.nil?
                          BSON::ObjectId.new
                        else
                          BSON::ObjectId.from_string(value)
                        end
              end
              [MongoBackend.field_path(@index, field).join('.'), value]
            end.compact]

            @client[@index.to_id_graph.key].update_one(
              { '_id' => values['_id'] },
              { '$set' => values },
              upsert: true
            )
          end
        end
      end

      # A query step to look up data from a particular collection
      class IndexLookupStatementStep < Backend::IndexLookupStatementStep
        # rubocop:disable Metrics/ParameterLists
        def initialize(client, select, conditions, step, next_step, prev_step)
          super

          @logger = Logging.logger['nose::backend::mongo::indexlookupstep']
          @order = @step.order_by.map do |field|
            { MongoBackend.field_path(@index, field).join('.') => 1 }
          end
        end
        # rubocop:enable Metrics/ParameterLists

        # Perform a column family lookup in MongoDB
        def process(conditions, results)
          results = initial_results(conditions) if results.nil?
          condition_list = result_conditions conditions, results

          new_result = condition_list.flat_map do |result_conditions|
            query_doc = query_doc_for_conditions result_conditions
            result = @client[@index.to_id_graph.key].find(query_doc)
            result = result.sort(*@order) unless @order.empty?

            result.to_a
          end

          # Limit the size of the results in case we fetched multiple keys
          new_result = new_result[0..(@step.limit.nil? ? -1 : @step.limit)]
          MongoBackend.rows_from_mongo new_result, @index, @step.fields
        end

        private

        # Produce the document used to issue the query to MongoDB
        # @return [Hash]
        def query_doc_for_conditions(conditions)
          conditions.map do |c|
            match = c.value
            match = BSON::ObjectId(match) if c.field.is_a? Fields::IDField

            # For range operators, find the corresponding MongoDB operator
            match = { mongo_operator(op) => match } if c.operator != :'='

            { MongoBackend.field_path(@index, c.field).join('.') => match }
          end.reduce(&:merge)
        end

        # Produce the comparison operator used in MongoDB
        # @return [String]
        def mongo_operator(operator)
          case operator
          when :>
            '$gt'
          when :>=
            '$gte'
          when :<
            '$lt'
          when :<=
            '$lte'
          end
        end
      end

      private

      # Create a Mongo client from the saved config
      def client
        @client ||= Mongo::Client.new @uri, database: @database
      end
    end
  end
end
