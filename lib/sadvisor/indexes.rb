require_relative 'node_extensions'

module Sadvisor
  # A representation of materialized views over fields in an entity
  class Index
    attr_reader :hash_fields, :order_fields, :extra, :all_fields, :path

    def initialize(hash_fields, order_fields, extra, path)
      @hash_fields = hash_fields.to_set
      @order_fields = order_fields
      @extra = extra.to_set
      @all_fields = (hash_fields + order_fields + extra).to_set
      @path = path

      # Initialize the hash function and freeze ourselves
      hash
      freeze
    end

    def state
      {
        hash_fields: @hash_fields.map(&:state),
        order_fields: @order_fields.map(&:state),
        extra: @extra.map(&:state),
        path: @path.map(&:state)
      }
    end

    # :nocov:
    def to_color
      hash_names = @hash_fields.map(&:inspect)
      order_names = @order_fields.map(&:inspect)
      extra_names = @extra.map(&:inspect)
      '[' + hash_names.join(', ') + ']' + \
        '[' + order_names.join(', ') + '] ' + \
        '→ [' + extra_names.join(', ') + ']' + \
        " $#{size}".yellow
    end
    # :nocov:

    # Two indices are equal if they contain the same fields
    # @return [Boolean]
    def ==(other)
      @hash_fields == other.hash_fields && \
          @order_fields == other.order_fields && \
          @extra.to_set == other.extra.to_set
    end
    alias_method :eql?, :==

    # Hash based on the fields, their keys, and the extra fields
    # @return [Fixnum]
    def hash
      @hash ||= [@hash_fields, @order_fields, @extra.to_set].hash
    end

    # Check if this index is a mapping from the key of the given entity
    # @see Entity#id_fields
    # @return [Boolean]
    def identity_for?(entity)
      @hash_fields == entity.id_fields.to_set
    end

    # Check if the index contains a given field
    # @return [Boolean]
    def contains_field?(field)
      @all_fields.include? field
    end

    # The size of a single entry in the index
    # @return [Fixnum]
    def entry_size
      @all_fields.map(&:size).inject(0, :+)
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
