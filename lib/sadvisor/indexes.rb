require_relative 'node_extensions'

module Sadvisor
  # A representation of materialized views over fields in an entity
  class Index
    attr_reader :fields, :extra, :path

    def initialize(fields, extra, path)
      @fields = fields
      @extra = extra
      @path = path

      # Track which key this field is mapped over
      # TODO: Chek if we still need field keys now that we have the path
      @field_keys = {}
      (@fields + @extra).each do |field|
        id_fields = field.parent.id_fields
        @field_keys[field] = id_fields ? id_fields[0..0] : []
      end
    end

    def state
      {
        fields: @fields.map(&:state),
        extra: @extra.map(&:state),
        path: @path.map(&:state)
      }
    end

    def to_color
      field_names = @fields.map(&:inspect)
      extra_names = @extra.map(&:inspect)
      '[' + field_names.join(', ') + '] → [' + extra_names.join(', ') + ']' + \
        " $#{size}".yellow
    end

    # Two indices are equal if they contain the same fields
    # @return [Boolean]
    def ==(other)
      @fields == other.fields && \
          @field_keys == other.instance_variable_get(:@field_keys) \
          && @extra.to_set == other.extra.to_set
    end
    alias_method :eql?, :==

    # Hash based on the fields, their keys, and the extra fields
    # @return [Fixnum]
    def hash
      @hash ||= [@fields, @field_keys, @extra.to_set].hash
    end

    # Get all the entities referenced in this index
    # @return [Array<Entity>]
    def entities
      (@fields + @extra).map(&:parent)
    end

    # Set the keys which a field in the index is derived from
    # This is useful when an entity may be reached via multiple foreign keys
    def set_field_keys(field, keys)
      @field_keys[field] = keys
    end

    # Get the keys which correspond to an entity for a given field
    def keys_for_field(field)
      @field_keys[field]
    end

    # Check if this index is a mapping from the key of the given entity
    # @see Entity#id_fields
    # @return [Boolean]
    def identity_for?(entity)
      @fields == entity.id_fields
    end

    # Check if the index contains a given field
    # @return [Boolean]
    def contains_field?(field)
      !(@fields + @extra).find { |index_field| field == index_field }.nil?
    end

    # Check if all the fields the query needs are indexed
    # @return [Boolean]
    def contains_fields?(fields, workload)
      fields.map do |field|
        contains_field?(workload.find_field field.value)
      end.all?
    end

    # The size of a single entry in the index
    # @return [Fixnum]
    def entry_size
      (@fields + @extra).map(&:size).inject(0, :+)
    end

    # The total size of this index
    # @return [Fixnum]
    def size
      fields.map(&:cardinality).inject(1, :*) * entry_size
    end
  end

  # Allow entities to create their own indices
  class Entity
    # Create a simple index which maps entity keys to other fields
    # @return [Index]
    def simple_index
      Index.new(id_fields, fields.values - id_fields, [self])
    end
  end
end

module CQL
  # Allow statements to materialize views
  class Statement
    # Construct an index which acts as a materialized view for a query
    # @return [Index]
    def materialize_view(workload)
      # Start with fields used for equality and range predicates, then order
      fields = eq_fields
      fields.push range_field if range_field

      fields = fields.map { |field| workload.find_field field.field.value }
      fields += order_by.map { |field| workload.find_field field }

      # Add all other fields used in the query, minus those already added
      extra = self.fields.map do |field|
        workload.find_field field.value
      end
      extra -= fields

      Sadvisor::Index.new(fields, extra,
                          longest_entity_path.reverse \
                            .map(&workload.method(:[])))
    end
  end
end
