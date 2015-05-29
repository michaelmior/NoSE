require 'mysql'

module NoSE
  module Loader
    # Load data from a MySQL database into a backend
    class MysqlLoader < LoaderBase
      def initialize(workload = nil, backend = nil)
        @workload = workload
        @backend = backend
      end

      # Load a generated set of indexes with data from MySQL
      def load(indexes, config, show_progress = false)
        client = new_client config

        indexes.each_with_index do |index, i|
          # Skip this index if it's not empty
          unless @backend.index_empty? index
            puts "Skipping index #{index.inspect}"
            next
          end

          sql = index_sql index, config[:limit]
          query = Mysql::Stmt.new client.protocol,
                                  Mysql::Charset.by_name('utf8')
          results = query.prepare(sql).execute

          if show_progress
            puts "Loading index #{i + 1}/#{indexes.count} #{index.inspect}"

            Formatador.new.redisplay_progressbar 0, results.size
            width = 50 - results.size.to_s.length * 2
            progress = Formatador::ProgressBar.new results.size,
                                                   started_at: Time.now,
                                                   width: width
          else
            progress = nil
          end

          results.each_hash.each_slice(100) do |chunk|
            chunk.map! { |row| convert_values row }
            @backend.index_insert_chunk index, chunk

            if show_progress
              inc = [progress.total - progress.current, 100].min
              progress.increment inc
            end
          end

          # Add a blank line to separate progress bars
          puts if progress
        end
      end

      # Read all tables in the database and construct a workload object
      def workload(config)
        client = new_client config

        workload = Workload.new
        client.query('SHOW TABLES').each do |table, |
          entity = Entity.new table
          entity.count = client.query("SELECT COUNT(*) FROM #{table}") \
              .first.first

          client.query("DESCRIBE #{table}").each do |name, type, _, key, _, _|
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
         Mysql.connect config[:host],
                       config[:username],
                       config[:password],
                       config[:database]
      end

      # Convert values to standard Ruby data types
      def convert_values(row)
        row.each do |key, value|
          case value
          when Mysql::Time
            row[key] = Time.at value.to_f
          end
        end
      end

      # Get all the fields selected by this index
      def index_sql_select(index)
        fields = index.hash_fields.to_a + index.order_fields + index.extra.to_a
        fields += index.path.last.id_fields

        fields.map do |field|
          "#{field.parent.name}.#{field.name} AS " \
          "#{field.parent.name}_#{field.name}"
        end
      end

      # Get the list of tables along with the join condition
      # for a query to fetch index data over a given set of keys
      def index_sql_tables(index, keys)
        tables = index.path.first.name
        ([nil] + keys).each_cons(2) do |prev_key, key|
          # Check which side of the entity we should be joining on
          if !prev_key.nil? && prev_key.parent == key.parent
            entity = key.entity
          else
            entity = key.parent
          end

          tables += " JOIN #{entity.name} ON " \
            "#{key.parent.name}.#{key.name}=" \
            "#{key.entity.name}.#{key.entity.id_fields.first.name}"
        end

        tables
      end

      # Construct a SQL statement to fetch the data to populate this index
      def index_sql(index, limit = nil)
        # Get all the necessary fields
        fields = index_sql_select index

        # Find the series of foreign keys along the index path
        keys = index.path.each_cons(2).map do |first, second|
          second.foreign_key_for(first) ||
          first.foreign_key_for(second)
        end

        # Construct the join condition
        tables = index_sql_tables index, keys

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
