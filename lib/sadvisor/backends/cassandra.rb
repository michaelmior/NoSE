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
          fnames = lambda do |fields, type|
            fields.map do |field|
              name = '"' + field.name + '"'
              name += ' ' + cassandra_type(field.class).to_s if type
              name
            end.join ', '
          end

          ddl = "CREATE COLUMNFAMILY \"#{index.key}\" (" \
          "#{fnames.call index.all_fields, true}, " \
          "PRIMARY KEY((#{fnames[index.hash_fields, false]})" \

          ddl += ", #{fnames[index.order_fields, false]}" \
            unless index.order_fields.empty?
          ddl += '));'

          enum.yield ddl
          client.execute(ddl) if execute
        end
      end
    end

    private

    # Get a Cassandra client, connecting if not done already
    def client
      @client ||= Cql::Client.connect hosts: @hosts, port: @port.to_s,
                                      keyspace: '"' + @keyspace + '"',
                                      consistency: :one
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
