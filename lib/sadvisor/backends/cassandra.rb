require 'cql'
require 'zlib'

module Sadvisor
  # A backend which communicates with Cassandra via CQL
  class CassandraBackend
    def initialize(workload, indexes, plans, config)
      @workload = workload
      @indexes = indexes
      @plans = plans

      @hosts = config[:hosts]
      @port = config[:port]
      @keyspace = config[:keyspace]
    end

    # Produce the DDL necessary for column families for the given indexes
    # and optionally execute them against the server
    def indexes_ddl(execute = false)
      Enumerator.new do |enum|
        @indexes.map do |index|
          # Add the ID of the last entity if necessary
          fields, extra_id = index_insert_fields index

          ddl = "CREATE COLUMNFAMILY \"#{index.key}\" (" \
          "#{field_names fields, true}, " \
          "PRIMARY KEY((#{field_names index.hash_fields})" \

          cluster_key = index.order_fields + extra_id
          ddl += ", #{field_names cluster_key}" unless cluster_key.empty?
          ddl += '));'

          enum.yield ddl
          client.execute(ddl) if execute
        end
      end
    end

    # Inset a chunk of rows into an index
    def index_insert_chunk(index, chunk)
      fields, _ = index_insert_fields index
      prepared = "INSERT INTO \"#{index.key}\" (" \
                 "#{field_names fields}" \
                 ") VALUES (#{(['?'] * fields.length).join ', '})"
      prepared = client.prepare prepared

      client.batch do |batch|
        chunk.each do |row|
          index_row = fields.map do |field|
            cassandra_value row["#{field.parent.name}_#{field.name}"],
                            field.class
          end
          batch.add prepared, *index_row
        end
      end
    end

    # Execute a query with the stored plans
    def query(query)
      plan = @plans.find { |possible_plan| possible_plan.query == query }

      results = nil
      ([nil] + plan.to_a + [nil]).each_cons(3) do |_prev_step, step, next_step|
        if step.is_a? IndexLookupPlanStep
          if results.nil?
            condition_list = [query.conditions]
          else
            # Get conditions based on hash keys of the next index
            # TODO: Support range queries and column name lookups
            condition_list = results.map do |result|
              step.index.hash_fields.map do |field|
                Condition.new field, :'=',
                              result["#{field.parent.name}_#{field.name}"]
              end
            end
          end

          if next_step.nil?
            select = query.select
          else
            # TODO: Select only necessary fields
            select = step.index.all_fields
          end

          results = index_lookup step.index, select, condition_list
          return [] if results.empty?
        elsif step.is_a? FilterStep
          fail NotImplementedError, 'Filtering is not yet implemented'
        elsif step.is_a? SortStep
          fail NotImplementedError, 'Sorting is not yet implemented'
        end
      end

      results
    end

    private

    # Add the ID of the last entity if necessary
    def index_insert_fields(index)
      extra_id = []
      extra_id += index.path.last.id_fields \
        unless (index.path.last.id_fields -
                (index.hash_fields.to_a + index.order_fields)).empty?
      fields = index.all_fields.to_set + extra_id.to_set

      [fields, extra_id]
    end

    # Get a comma-separated list of field names with optional types
    def field_names(fields, types = false)
      fields.map do |field|
        name = "\"#{field_name field}\""
        name += ' ' + cassandra_type(field.class).to_s if types
        name
      end.join ', '
    end

    # Get the name of the field as used in Cassandra
    def field_name(field)
      "#{field.parent.name}_#{field.name}"
    end

    # Get a Cassandra client, connecting if not done already
    def client
      @client ||= Cql::Client.connect hosts: @hosts, port: @port.to_s,
                                      keyspace: '"' + @keyspace + '"',
                                      default_consistency: :one
    end

    # Return the datatype to use in Cassandra for a given field
    def cassandra_type(field_class)
      case [field_class]
      when [IntegerField]
        :int
      when [FloatField]
        :float
      when [StringField]
        :text
      when [DateField]
        :timestamp
      when [IDField], [ForeignKeyField], [ToOneKeyField], [ToManyKeyField]
        # TODO: Decide on UUID
        :int
      end
    end

    # Get the value as used by Cassandra for a given field
    def cassandra_value(value, field_class)
      # case [field_class]
      # when [IDField], [ForeignKeyField], [ToOneKeyField], [ToManyKeyField]
      #   Cql::Uuid.new Zlib.crc32(value.to_s)
      # else
      #   value
      # end

      # TODO: Decide on the use of UUID
      value
    end

    # Lookup values from an index selecting the given
    # fields and filtering on the given conditions
    def index_lookup(index, select, condition_list)
      query = "SELECT #{select.map { |field| field_name field }.join ', '} " \
              "FROM \"#{index.key}\""
      query += ' WHERE ' if condition_list.first.length > 0
      query += condition_list.first.map do |condition|
        "#{field_name condition.field} #{condition.operator} ?"
      end.join ', '
      statement = client.prepare query

      # TODO: Chain enumerables of results instead
      result = []
      condition_list.each do |conditions|
        values = conditions.map do |condition|
          cassandra_value condition.value, condition.field.class
        end

        result += statement.execute(*values, consistency: :one).to_a
      end

      result
    end
  end
end
