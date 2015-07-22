require 'cassandra'
require 'zlib'

module NoSE
  module Backend
    # A backend which communicates with Cassandra via CQL
    class CassandraBackend < BackendBase
      def initialize(workload, indexes, plans, update_plans, config)
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
            value = row["#{field.parent.name}_#{field.name}"]
            value = Cassandra::Uuid.new(value.to_i) \
              if field.is_a?(Fields::IDField)

            value
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
        rows = client.execute(query).rows
        fail if rows.any? { |row| row.values.any?(&:nil?) }
        rows
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
          :uuid
        end
      end

      # Insert data into an index on the backend
      class InsertStatementStep < StatementStep
        @prepared = {}

        def self.process(client, index, results)
          fields = results.first.keys.sort
          @prepared[[index, fields]] ||= self.prepare client, index, fields
          statement = @prepared[[index, fields]]

          # Insert each row into the index
          generator = Cassandra::Uuid::Generator.new
          results.each do |result|
            values = fields.map do |key|
              cur_field = index.all_fields.find { |field| field.id == key }
              value = result[key]

              # If this is an ID, generate or construct a UUID object
              if cur_field.is_a?(Fields::IDField)
                value = value.nil? ? generator.uuid : \
                                     Cassandra::Uuid.new(value.to_i)
              end

              value
            end
            client.execute(statement, *values)
          end
        end

        private

        def self.prepare(client, index, fields)
          # Prepare the statement required to perform the insertion
          insert_keys = fields & index.all_fields.map(&:id)
          insert = "INSERT INTO #{index.key} ("
          insert += insert_keys.join(', ') + ') VALUES ('
          insert += (['?'] * insert_keys.length).join(', ') + ')'
          client.prepare insert
        end
      end

      # Delete data from an index on the backend
      class DeleteStatementStep < StatementStep
        @prepared = {}

        # Execute the delete for a given set of keys
        def self.process(client, index, results)
          @prepared[index] ||= self.prepare client, index
          statement = @prepared[index]

          # Delete each row from the index
          index_keys = index.hash_fields + index.order_fields.to_set
          results.each do |result|
            values = index_keys.map do |key|
              cur_field = index.all_fields.find { |field| field.id == key.id }
              cur_field.is_a?(Fields::IDField) ? \
                Cassandra::Uuid.new(result[key.id].to_i): result[key.id]
            end
            client.execute(statement, *values)
          end
        end

        private

        # Prepare the statement to execute the query
        def self.prepare(client, index)
          # Prepare the statement required to perform the deletion
          index_keys = index.hash_fields + index.order_fields.to_set
          delete = "DELETE FROM #{index.key} WHERE "
          delete += index_keys.map { |key| "#{key.id} = ?" }.join ' AND '
          client.prepare delete
        end
      end

      # A query step to look up data from a particular column family
      class IndexLookupStatementStep < StatementStep
        @prepared = {}
        @logger = Logging.logger['nose::backend::cassandra::indexlookupstep']

        # Perform a column family lookup in Cassandra
        def self.process(client, query, results, step, prev_step, next_step)
          @prepared[[query, step.index]] ||= self.prepare client, query, step,
                                                           prev_step, next_step
          statement = @prepared[[query, step.index]]

          # If this is the first lookup, get the lookup values from the query
          if results.nil?
            results = [Hash[query.conditions.map do |field_id, condition|
              fail if condition.value.nil?
              [field_id, condition.value]
            end]]
          end

          # Construct a list of conditions from the results

          # TODO: Check if we can apply the next filter via ALLOW FILTERING
          eq_fields = (prev_step.state.eq - step.state.eq).to_set
          eq_fields += step.index.hash_fields
          range_field = prev_step.state.range if step.state.range.nil?

          condition_list = results.map do |result|
            conditions = eq_fields.map do |field|
              Condition.new field, :'=', result[field.id]
            end

            unless range_field.nil?
              operator = query.conditions.values.find(&:range?).operator
              conditions << Condition.new(range_field, operator,
                                          result[range_field.id])
            end

            conditions
          end

          # Lookup values from an index selecting the given
          # fields and filtering on the given conditions
          # TODO: Chain enumerables of results instead
          result = []
          @logger.debug { "  #{statement.cql} * #{condition_list.size}" }
          condition_list.each do |conditions|
            values = conditions.map do |condition| value = condition.value ||
                      query.conditions[condition.field.id].value
              fail if value.nil?
              condition.field.is_a?(Fields::IDField) ? \
                Cassandra::Uuid.new(value.to_i): value
            end

            # Loop over all pages to fetch results
            new_results = client.execute(statement, *values)
            loop do
              rows = new_results.to_a
              fail if rows.any? { |row| row.values.any?(&:nil?) }
              result += rows
              break if new_results.last_page?
              new_results = new_results.next_page
              @logger.debug "Fetched #{result.length} results"
            end
          end
          @logger.debug "Total result size = #{result.size}"

          result
        end

        private

        # Construct a prepared statement for the query
        def self.prepare(client, query, step, prev_step, next_step)
          # Decide which fields should be selected
          # We just pick whatever is contained in the index that is either
          # mentioned in the query or required for the next lookup
          # TODO: Potentially try query.all_fields for those not required
          #       It should be sufficient to check what is needed for future
          #       filtering and sorting and use only those + query.select
          select = query.all_fields
          select += next_step.index.hash_fields \
            unless next_step.nil? ||
              !next_step.is_a?(Plans::IndexLookupPlanStep)
          select &= step.index.all_fields

          # XXX Duplicated from #process
          eq_fields = (prev_step.state.eq - step.state.eq).to_set
          eq_fields += step.index.hash_fields
          range_field = prev_step.state.range if step.state.range.nil?

          cql = "SELECT #{select.map(&:id).join ', '} FROM " \
                "\"#{step.index.key}\" WHERE "
          cql += eq_fields.map do |field|
            "#{field.id} = ?"
          end.join ' AND '
          unless range_field.nil?
            condition = query.conditions.values.find(&:range?)
            cql += " AND #{condition.field.id} #{condition.operator} ?"
          end

          # Add the ORDER BY clause
          # TODO: CQL3 requires all clustered columns before the one actually
          #       ordered on also be specified
          #
          #       Example:
          #
          #         SELECT * FROM cf WHERE id=? AND col1=? ORDER by col1, col2
          cql += ' ORDER BY ' + step.order_by.map(&:id).join(', ') \
            unless step.order_by.empty?

          # Add an optional limit
          cql += " LIMIT #{step.limit}" unless step.limit.nil?

          client.prepare cql
        end
      end
    end
  end
end
