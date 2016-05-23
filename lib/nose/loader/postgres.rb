require 'pg'

module NoSE
  module Loader
    # Load data from a PostgreSQL database into a backend
    class PostgresLoader < LoaderBase
      def initialize(workload = nil, backend = nil)
        @logger = Logging.logger['nose::loader::postgresloader']

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

          sql, = index_sql index, limit
          results = client.exec(sql)

          result_chunk = []
          results.each do |result|
            result.each do |key, value|
              result[key] = index[key].class.value_from_string value
            end

            result_chunk.push result
            if result_chunk.length >= 100
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
        PG.connect host: config[:host],
                   dbname: config[:database],
                   user: config[:username],
                   password: config[:password]
      end

      # Get all the fields selected by this index
      def index_sql_select(index)
        fields = index.hash_fields.to_a + index.order_fields + index.extra.to_a
        fields += index.path.entities.last.id_fields

        [fields, fields.map do |field|
          "#{field.parent.name}.#{field.name} AS " \
          "#{field.parent.name}_#{field.name}"
        end]
      end

      # Get the list of tables along with the join condition
      # for a query to fetch index data
      def index_sql_tables(index)
        # Create JOIN statements
        tables = index.path.entities.map(&:name).join ' JOIN '
        return tables if index.path.length == 1

        tables += ' WHERE '
        tables += index.path.each_cons(2).map do |_prev_key, key|
          key = key.reverse if key.relationship == :many
          "#{key.parent.name}.#{key.name}=" \
            "#{key.entity.name}.#{key.entity.id_fields.first.name}"
        end.join ' AND '

        tables
      end

      # Construct a SQL statement to fetch the data to populate this index
      def index_sql(index, limit = nil)
        # Get all the necessary fields
        fields, select = index_sql_select index

        # Construct the join condition
        tables = index_sql_tables index

        query = "SELECT #{select.join ', '} FROM #{tables}"
        query += " LIMIT #{limit}" unless limit.nil?

        @logger.debug query
        [query, fields]
      end
    end
  end
end
