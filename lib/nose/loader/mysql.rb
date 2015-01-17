require 'mysql'

module NoSE::Loader
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
        sql = index_sql index
        query = Mysql::Stmt.new client.protocol, Mysql::Charset.by_name('utf8')
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

        results.each_hash.each_slice(1000) do |chunk|
          Parallel.each(chunk.each_slice(100),
                        finish: (lambda do |_, _, _|
                          # Update the progress bar
                          if progress
                            inc = [progress.total - progress.current, 100].min
                            progress.increment inc
                          end
                        end)) do |minichunk|
            @backend.index_insert_chunk index, minichunk
          end
        end

        # Add a blank line to separate progress bars
        puts if progress
      end
    end

    # Read all tables in the database and construct a workload object
    def workload(config)
      client = new_client config

      workload = NoSE::Workload.new
      client.query('SHOW TABLES').each do |table, |
        entity = NoSE::Entity.new table
        entity.count = client.query("SELECT COUNT(*) FROM #{table}") \
            .first.first

        client.query("DESCRIBE #{table}").each do |name, type, _, key, _, _|
          if key == 'PRI'
            field_class = NoSE::Fields::IDField
          else
            case type
            when /datetime/
              field_class = NoSE::Fields::DateField
            when /float/
              field_class = NoSE::Fields::FloatField
            when /text/
              # TODO: Get length
              field_class = NoSE::Fields::StringField
            when /varchar\(([0-9]+)\)/
              # TODO: Use length
              field_class = NoSE::Fields::StringField
            when /(tiny)?int/
              field_class = NoSE::Fields::IntegerField
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

    # Construct a SQL statement to fetch the data to populate this index
    def index_sql(index)
      # Get all the necessary fields
      fields = index.hash_fields.to_a + index.order_fields + index.extra.to_a
      fields += index.path.last.id_fields
      fields = fields.map do |field|
        "#{field.parent.name}.#{field.name} AS " \
        "#{field.parent.name}_#{field.name}"
      end

      # Find the series of foreign keys along the index path
      keys = index.path.each_cons(2).map do |first, second|
        second.foreign_key_for first
      end

      # Construct the join condition
      tables = index.path.first.name
      keys.each do |key|
        tables += " JOIN #{key.parent.name} ON " \
                  "#{key.parent.name}.#{key.name}=" \
                  "#{key.entity.name}.#{key.entity.id_fields.first.name}"
      end

      "SELECT #{fields.join ', '} FROM #{tables}"
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
