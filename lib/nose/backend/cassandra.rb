require 'cassandra'
require 'zlib'

module NoSE
  module Backend
    # A backend which communicates with Cassandra via CQL
    class CassandraBackend < BackendBase
      def initialize(workload, indexes, plans, config)
        super

        @hosts = config[:hosts]
        @port = config[:port]
        @keyspace = config[:keyspace]
      end

      # Produce the DDL necessary for column families for the given indexes
      # and optionally execute them against the server
      def indexes_ddl(execute = false, skip_existing = false,
                      drop_existing = false)
        Enumerator.new do |enum|
          @indexes.map do |index|
            ddl = "CREATE COLUMNFAMILY \"#{index.key}\" (" \
                  "#{field_names index.all_fields, true}, " \
                  "PRIMARY KEY((#{field_names index.hash_fields})"

            cluster_key = index.order_fields
            ddl += ", #{field_names cluster_key}" unless cluster_key.empty?
            ddl += '));'

            enum.yield ddl

            begin
              drop_index(index) if drop_existing && index_exists?(index)
              client.execute(ddl) if execute
            rescue Cassandra::Errors::AlreadyExistsError => exc
              next if skip_existing

              new_exc = IndexAlreadyExists.new exc.message
              new_exc.set_backtrace exc.backtrace
              raise new_exc
            end
          end
        end
      end

      # Insert a chunk of rows into an index
      def index_insert_chunk(index, chunk)
        fields = index.all_fields
        prepared = "INSERT INTO \"#{index.key}\" (" \
                   "#{field_names fields}" \
                   ") VALUES (#{(['?'] * fields.length).join ', '})"
        prepared = client.prepare prepared

        futures = []
        chunk.each do |row|
          index_row = fields.map do |field|
            row["#{field.parent.name}_#{field.name}"]
          end

          futures.push client.execute_async(prepared, *index_row)
        end
        futures.each(&:join)
      end

      # Check if the given index is empty
      def index_empty?(index)
        query = "SELECT COUNT(*) FROM \"#{index.key}\" LIMIT 1"
        client.execute(query).first.values.first == 0
      end

      # Check if a given index exists in the target database
      def index_exists?(index)
        query = 'SELECT COUNT(*) FROM system.schema_columnfamilies ' \
                "WHERE keyspace_name='#{@keyspace}' AND " \
                "columnfamily_name='#{index.key}';"
        client.execute(query).first.values.first > 0
      end

      # Check if a given index exists in the target database
      def drop_index(index)
        client.execute "DROP TABLE \"#{index.key}\""
      end

      # Sample a number of values from the given index
      def index_sample(index, count)
        query = "SELECT #{index.all_fields.map(&:id).join ', '} " \
                "FROM \"#{index.key}\" LIMIT #{count}"
        client.execute(query).rows
      end

      private

      # Get a comma-separated list of field names with optional types
      def field_names(fields, types = false)
        fields.map do |field|
          name = "\"#{field.id}\""
          name += ' ' + cassandra_type(field.class).to_s if types
          name
        end.join ', '
      end

      # Get a Cassandra client, connecting if not done already
      def client
        return @client unless @client.nil?
        cluster = Cassandra.cluster hosts: @hosts, port: @port.to_s
        @client = cluster.connect @keyspace
      end

      # Return the datatype to use in Cassandra for a given field
      def cassandra_type(field_class)
        case [field_class]
        when [Fields::IntegerField]
          :int
        when [Fields::FloatField]
          :float
        when [Fields::StringField]
          :text
        when [Fields::DateField]
          :timestamp
        when [Fields::IDField],
             [Fields::ForeignKeyField]
          # TODO: Decide on UUID
          :int
        end
      end

      # A query step to look up data from a particular column family
      class IndexLookupQueryStep < QueryStep
        # Perform a column family lookup in Cassandra
        def self.process(client, query, results, step, prev_step, next_step)
          # Get the fields which are used for lookups at this step
          # TODO: Check if we can apply the next filter via ALLOW FILTERING
          eq_fields = (prev_step.state.eq - step.state.eq).to_set
          eq_fields += step.index.hash_fields
          range_field = prev_step.state.range if step.state.range.nil?

          # If this is the first lookup, get the lookup values from the query
          if results.nil?
            results = [Hash[query.conditions.map do |condition|
              [condition.field.id, condition.value]
            end]]
          end

          # Construct a list of conditions from the results
          condition_list = results.map do |result|
            conditions = eq_fields.map do |field|
              Condition.new field, :'=', result[field.id]
            end

            unless range_field.nil?
              operator = query.conditions.select(&:range?).first.operator
              conditions << Condition.new(range_field, operator,
                                          result[range_field.id])
            end

            conditions
          end

          # Decide which fields should be selected
          # We just pick whatever is contained in the index that is either
          # mentioned in the query or required for the next lookup
          # TODO: Potentially try query.all_fields for those not required
          #       It should be sufficient to check what is needed for future
          #       filtering and sorting and use only those + query.select
          select = query.all_fields
          select += next_step.index.hash_fields \
            unless next_step.nil? || !next_step.is_a?(IndexLookupPlanStep)
          select &= step.index.all_fields

          results = index_lookup client, step.index, select, condition_list,
                                 step.order_by, step.limit
          return [] if results.empty?

          results
        end

        private

        # Lookup values from an index selecting the given
        # fields and filtering on the given conditions
        def self.index_lookup(client, index, select, condition_list,
                              order, limit)
          query = "SELECT #{select.map(&:id).join ', '} FROM \"#{index.key}\""
          query += ' WHERE ' if condition_list.first.length > 0
          query += condition_list.first.map do |condition|
            "#{condition.field.id} #{condition.operator} ?"
          end.join ' AND '

          # Add the ORDER BY clause
          # TODO: CQL3 requires all clustered columns before the one actually
          #       ordered on also be specified
          #
          #       Example:
          #
          #         SELECT * FROM cf WHERE id=? AND col1=? ORDER by col1, col2
          unless order.empty?
            query += ' ORDER BY ' + order.map(&:id).join(', ')
          end

          # Add an optional limit
          query += " LIMIT #{limit}" unless limit.nil?

          statement = client.prepare query

          # TODO: Chain enumerables of results instead
          result = []
          condition_list.each do |conditions|
            values = conditions.map(&:value)
            result += client.execute(statement, *values).to_a
          end

          result
        end
      end
    end
  end
end
