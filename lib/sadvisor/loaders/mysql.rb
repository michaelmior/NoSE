require 'mysql2'

module Sadvisor
  class MySQLLoader < Loader
    def initialize(workload, backend)
      @workload = workload
      @backend = backend
    end

    def load(indexes, config, show_progress = false)
      client = Mysql2::Client.new config
      indexes.each_with_index do |index, i|
        sql = index_sql index
        results = client.query(sql)

        if show_progress
          puts "Loading index #{i + 1}/#{indexes.count} #{index.inspect}"

          Formatador.new.redisplay_progressbar 0, results.count
          progress = Formatador::ProgressBar.new results.count,
                                                 started_at: Time.now
        else
          progress = nil
        end

        results.each_slice(1000) do |chunk|
          Parallel.each(chunk.each_slice(100),
                        finish: (lambda do |_, _, _|
                          inc = [progress.total - progress.current, 100].min
                          progress.increment inc if progress
                        end)) do |minichunk|
            @backend.index_insert_chunk index, minichunk
          end
        end
      end
    end

    private

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
