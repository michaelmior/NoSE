require_relative 'node_extensions'

module Sadvisor
  # A representation of materialized views over fields in an entity
  class Index
    attr_reader :hash_fields, :order_fields, :extra, :path

    def initialize(hash_fields, order_fields, extra, path)
      @hash_fields = hash_fields
      @order_fields = order_fields
      @extra = extra
      @path = path

      # Track which key this field is mapped over
      # TODO: Check if we still need field keys now that we have the path
      @field_keys = {}
      (@hash_fields + @order_fields + @extra).each do |field|
        id_fields = field.parent.id_fields
        @field_keys[field] = id_fields ? id_fields[0..0] : []
      end
    end

    def state
      {
        hash_fields: @hash_fields.map(&:state),
        order_fields: @order_fields.map(&:state),
        extra: @extra.map(&:state),
        path: @path.map(&:state)
      }
    end

    def to_color
      field_names = @hash_fields.map(&:inspect) + @order_fields.map(&:inspect)
      extra_names = @extra.map(&:inspect)
      '[' + field_names.join(', ') + '] → [' + extra_names.join(', ') + ']' + \
        " $#{size}".yellow
    end

    # Two indices are equal if they contain the same fields
    # @return [Boolean]
    def ==(other)
      @hash_fields == other.hash_fields && \
          @order_fields == other.order_fields && \
          @field_keys == other.instance_variable_get(:@field_keys) \
          && @extra.to_set == other.extra.to_set
    end
    alias_method :eql?, :==

    # Hash based on the fields, their keys, and the extra fields
    # @return [Fixnum]
    def hash
      @hash ||= [@hash_fields, @order_fields, @field_keys, @extra.to_set].hash
    end

    # Get all the entities referenced in this index
    # @return [Array<Entity>]
    def entities
      (@hash_fields + @order_fields + @extra).map(&:parent)
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
      @hash_fields == entity.id_fields
    end

    # Check if the index contains a given field
    # @return [Boolean]
    def contains_field?(field)
      !(@hash_fields + @order_fields + @extra).find do |index_field|
        field == index_field
      end.nil?
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
      (@hash_fields + @order_fields + @extra).map(&:size).inject(0, :+)
    end

    # The total size of this index
    # @return [Fixnum]
    def size
      (@hash_fields + @order_fields).map(&:cardinality) \
        .inject(1, :*) * entry_size
    end
  end

  # Allow entities to create their own indices
  class Entity
    # Create a simple index which maps entity keys to other fields
    # @return [Index]
    def simple_index
      Index.new(id_fields, [], fields.values - id_fields, [self])
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
      order_fields = []
      order_fields.push workload.find_field(range_field.field.value) \
        if range_field
      order_fields += order_by.map { |field| workload.find_field field }

      hash_fields = eq_fields.map do |field|
        workload.find_field field.field.value
      end

      # Add all other fields used in the query, minus those already added
      extra = fields.map do |field|
        workload.find_field field.value
      end
      extra -= (hash_fields + order_fields)

      Sadvisor::Index.new(hash_fields, order_fields, extra,
                          longest_entity_path.reverse \
                            .map(&workload.method(:[])))
    end
  end
end
