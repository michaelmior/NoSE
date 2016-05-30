require 'sequel'

module NoSE
  module Loader
    # Load data from a MySQL database into a backend
    class SqlLoader < LoaderBase
      def initialize(workload = nil, backend = nil)
        @logger = Logging.logger['nose::loader::sqlloader']

        @workload = workload
        @backend = backend
      end

      # Load a generated set of indexes with data from MySQL
      def load(indexes, config, show_progress = false, limit = nil,
               skip_existing = true)
        # XXX Assuming backend is thread-safe
        Parallel.each(indexes, in_threads: 2) do |index|
          client = new_client config

          # Skip this index if it's not empty
          if skip_existing && !@backend.index_empty?(index)
            @logger.info "Skipping index #{index.inspect}" if show_progress
            next
          end
          @logger.info "#{index.inspect}" if show_progress

          query, fields = index_sql client, index, limit

          result_chunk = []
          query.each do |result|
            result = Hash[result.map { |k, v| [k.to_s, v] }]
            result_chunk.push result
            if result_chunk.length >= 100
              exit
              @backend.index_insert_chunk index, result_chunk
              result_chunk = []
            end
          end
          @backend.index_insert_chunk index, result_chunk \
            unless result_chunk.empty?
        end
      end

      private

      # Create a new client from the given configuration
      def new_client(config)
        Sequel.connect config[:uri]
      end

      # Get all the fields selected by this index
      def index_sql_select(index)
        fields = index.hash_fields.to_a + index.order_fields + index.extra.to_a
        fields += index.path.entities.last.id_fields

        [fields, fields.map do |field|
          "#{field.parent.name}__#{field.name}___" \
            "#{field.parent.name}_#{field.name}".to_sym
        end]
      end

      # Get the list of tables along with the join condition
      # for a query to fetch index data
      def index_sql_tables(index)
        # Create JOIN statements
        tables = index.path.entities.map { |entity| entity.name.to_sym }
        return [tables, []] if index.path.length == 1

        keys = index.path.each_cons(2).map do |_prev_key, key|
          is_many = key.relationship == :many
          key = key.reverse if is_many
          fields = [key.entity.id_fields.first.name.to_sym, key.name.to_sym]
          fields = fields.reverse if is_many
          Hash[[fields]]
        end

        [tables, keys]
      end

      # Construct a SQL statement to fetch the data to populate this index
      def index_sql(client, index, limit = nil)
        # Get all the necessary fields
        fields, select = index_sql_select index

        # Construct the join condition
        tables, keys = index_sql_tables index

        query = client[tables.first]
        keys.each_with_index.map do |key, i|
          query = query.join tables[i + 1], key
        end

        query = query.select(*select)
        query = query.limit limit unless limit.nil?

        @logger.debug { query.sql }
        [query, fields]
      end
    end
  end
end
