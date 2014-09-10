module Sadvisor
  # A backend which communicates with Cassandra via CQL
  class CassandraBackend
    def initialize(workload, indexes, plans, **_options)
      @workload = workload
      @indexes = indexes
      @plans = plans
    end

    # Produce the DDL necessary for column families for the given indexes
    def indexes_ddl
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

        ddl
      end
    end

    private

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
