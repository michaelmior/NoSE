require 'mysql2'

module NoSE
  module Loader
    # Load data from a MySQL database into a backend
    class MysqlLoader < LoaderBase
      def initialize(workload = nil, backend = nil)
        @workload = workload
        @backend = backend
      end

      # Load a generated set of indexes with data from MySQL
      def load(indexes, config, show_progress = false, limit = nil)
        # XXX Assuming backend is thread-safe
        Parallel.each(indexes, in_threads: 2) do |index|
          client = new_client config

          # Skip this index if it's not empty
          unless @backend.index_empty? index
            puts "Skipping index #{index.inspect}"
            next
          end
          puts "#{index.inspect}" if show_progress

          sql = index_sql index, limit
          results = client.query(sql, stream: true, cache_rows: false)

          result_chunk = []
          results.each do |result|
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

      # Read all tables in the database and construct a workload object
      def workload(config)
        client = new_client config

        workload = Workload.new
        client.query('SHOW TABLES').each(as: :array) do |table, |
          entity = Entity.new table
          entity.count = client.query("SELECT COUNT(*) FROM #{table}") \
            .first.values.first

          describe = client.query("DESCRIBE #{table}")
          describe.each(as: :array) do |name, type, _, key, _, _|
            if key == 'PRI'
              field_class = Fields::IDField
            else
              case type
              when /datetime/
                field_class = Fields::DateField
              when /float/
                field_class = Fields::FloatField
              when /text/
                # TODO: Get length
                field_class = Fields::StringField
              when /varchar\(([0-9]+)\)/
                # TODO: Use length
                field_class = Fields::StringField
              when /(tiny)?int/
                field_class = Fields::IntegerField
              end
            end

            entity << field_class.new(name)
          end

          workload << entity
          # TODO: Handle foreign keys
        end

        workload
      end

      private

      # Create a new client from the given configuration
      def new_client(config)
         Mysql2::Client.new host: config[:host],
                            username: config[:username],
                            password: config[:password],
                            database: config[:database]
      end

      # Get all the fields selected by this index
      def index_sql_select(index)
        fields = index.hash_fields.to_a + index.order_fields + index.extra.to_a
        fields += index.path.entities.last.id_fields

        fields.map do |field|
          "#{field.parent.name}.#{field.name} AS " \
          "#{field.parent.name}_#{field.name}"
        end
      end

      # Get the list of tables along with the join condition
      # for a query to fetch index data
      def index_sql_tables(index)
        # Create JOIN statements
        tables = index.path.entities.map(&:name).join ' JOIN '
        return tables if index.path.length == 1

        tables += ' WHERE '
        tables += index.path.each_cons(2).map do |prev_key, key|
          key = key.reverse if key.relationship == :one
          "#{key.parent.name}.#{key.name}=" \
            "#{key.entity.name}.#{key.entity.id_fields.first.name}"
        end.join ' AND '

        tables
      end

      # Construct a SQL statement to fetch the data to populate this index
      def index_sql(index, limit = nil)
        # Get all the necessary fields
        fields = index_sql_select index

        # Construct the join condition
        tables = index_sql_tables index

        query = "SELECT #{fields.join ', '} FROM #{tables}"
        query += " LIMIT #{limit}" unless limit.nil?

        query
      end
    end
  end
end

class Mysql
  # Simple addition of to_f for value serialization
  class Time
    # Return the time as milliseconds since the epoch
    def to_f
      ::Time.new(@year, @month, @day, @hour, @minute, @second).to_f
    end
  end
end
