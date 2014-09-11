require 'cql'

module Sadvisor
  # A backend which communicates with Cassandra via CQL
  class CassandraBackend
    def initialize(workload, indexes, plans, **options)
      @workload = workload
      @indexes = indexes
      @plans = plans

      @hosts = options[:backend]['hosts']
      @port = options[:backend]['port']
      @keyspace = options[:backend]['keyspace']
    end

    # Produce the DDL necessary for column families for the given indexes
    # and optionally execute them against the server
    def indexes_ddl(execute = false)
      Enumerator.new do |enum|
        @indexes.map do |index|
          ddl = "CREATE COLUMNFAMILY \"#{index.key}\" (" \
          "#{field_names index.all_fields, true}, " \
          "PRIMARY KEY((#{field_names index.hash_fields})" \

          ddl += ", #{field_names index.order_fields}" \
            unless index.order_fields.empty?
          ddl += '));'

          enum.yield ddl
          client.execute(ddl) if execute
        end
      end
    end

    # Inset a chunk of rows into an index
    def index_insert_chunk(index, chunk)
      prepared = "INSERT INTO \"#{index.key}\" (" \
                 "#{field_names index.all_fields}" \
                 ") VALUES (#{(['?'] * index.all_fields.length).join ', '})"
      prepared = client.prepare prepared

      client.batch do |batch|
        chunk.each do |row|
          index_row = index.all_fields.map do |field|
            row["#{field.parent.name}_#{field.name}"]
          end
          batch.add prepared, *index_row
        end
      end
    end

    private

    # Get a comma-separated list of field names with optional types
    def field_names(fields, types = false)
      fields.map do |field|
        name = "\"#{field.parent.name}_#{field.name}\""
        name += ' ' + cassandra_type(field.class).to_s if types
        name
      end.join ', '
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
        :uuid
      end
    end
  end
end
