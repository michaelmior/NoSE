require_relative 'node_extensions'

module Sadvisor
  # A representation of materialized views over fields in an entity
  class Index
    attr_reader :hash_fields, :order_fields, :extra, :all_fields, :path

    def initialize(hash_fields, order_fields, extra, path)
      @hash_fields = hash_fields.to_set
      @order_fields = order_fields
      @extra = extra.to_set
      @all_fields = @hash_fields + order_fields.to_set + @extra
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
      hash == other.hash
    end
    alias_method :eql?, :==

    # Hash based on the fields, their keys, and the path
    # @return [Fixnum]
    def hash
      @hash ||= [@hash_fields, @order_fields, @extra.to_set, @path].hash
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

    # Create a new range over the entities traversed by an index using
    # the numerical indices into a list of entities
    def entity_range(entities)
      Range.new(*(@path.map do |entity|
        entities.index entity.name
      end).minmax) rescue (nil..nil)
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

  # Allow statements to materialize views
  class Statement
    # Construct an index which acts as a materialized view for a query
    # @return [Index]
    def materialize_view
      order_fields = @order
      if @range_field && !@order.include?(@range_field)
        order_fields << @range_field
      end

      Sadvisor::Index.new(@eq_fields, order_fields,
                          all_fields - (@eq_fields + @order).to_set,
                          @longest_entity_path.reverse)
    end
  end
end
