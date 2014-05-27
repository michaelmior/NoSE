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
      @field_keys = {}
      (@fields + @extra).each do |field|
        id_fields = field.parent.id_fields
        @field_keys[field] = id_fields ? id_fields[0..0] : []
      end
    end

    def to_color
      field_names = @fields.map(&:inspect)
      extra_names = @extra.map(&:inspect)
      '[' + field_names.join(', ') + '] → [' + extra_names.join(', ') + ']' + \
        " $#{size}".yellow
    end

    # Two indices are equal if they contain the same fields
    def ==(other)
      @fields == other.fields && \
          @field_keys == other.instance_variable_get(:@field_keys) \
          && @extra.to_set == other.extra.to_set
    end

    def eql?(other)
      self == other
    end

    def hash
      [@fields, @field_keys, @extra.to_set].hash
    end

    # Get all the entities referenced in this index
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
    def identity_for?(entity)
      @fields == entity.id_fields
    end

    # Check if the index contains a given field
    def contains_field?(field)
      !(@fields + @extra).find { |index_field| field == index_field }.nil?
    end

    # Check if all the fields the query needs are indexed
    def contains_fields?(fields, workload)
      fields.map do |field|
        contains_field?(workload.find_field field.value)
      end.all?
    end

    # The size of a single entry in the index
    def entry_size
      (@fields + @extra).map(&:size).inject(0, :+)
    end

    # The total size of this index
    def size
      fields.map(&:cardinality).inject(1, :*) * entry_size
    end

    # Check if an index can support a given query
    # @see #supports_predicates?
    def supports_query?(query, workload)
      supports_predicates? query.from.value, query.fields, query.eq_fields, \
                           query.range_field, query.order_by, workload
    end

    private

    # Check if the index supports the given range predicate
    def supports_range?(range, workload)
      if range
        range_field = workload.find_field range.field.value
        @fields.last == range_field
      else
        true
      end
    end

    # Check if the index supports ordering by the given list of fields
    def supports_order?(order_by, workload)
      # XXX Need to consider when these fields are not the last
      order_fields = order_by.map { |field| workload.find_field field }
      order_fields.length == 0 || \
        order_fields == @fields[-order_fields.length..-1]
    end

    # Check if the given predicates can be supported by this index
    def supports_predicates?(from, fields, eq, range, order_by, workload)
      # Ensure all the fields the query needs are indexed
      return false unless contains_fields? fields, workload

      # Track fields used in predicates
      predicate_fields = []

      # Range predicates must occur last
      return false unless supports_range? range, workload
      predicate_fields.push range.field.value if range

      # All fields in the where clause must be indexes
      return false unless eq.map do |condition|
        @fields.include?(workload.find_field condition.field.value)
      end.all?
      predicate_fields += eq.map { |field| field.field.value }

      # Fields for ordering must appear last
      return false unless supports_order? order_by, workload
      predicate_fields += order_by

      from_entity = workload.entities[from]
      return false unless predicate_fields.map do |field|
        keys_for_field(workload.find_field field) == \
          from_entity.key_fields(field)
      end.all?

      true
    end
  end

  # Allow entities to create their own indices
  class Entity
    # Create a simple index which maps entity keys to other fields
    def simple_index
      Index.new(id_fields, fields.values - id_fields, [self])
    end
  end
end

module CQL
  # Allow statements to materialize views
  class Statement
    # Construct an index which acts as a materialized view for a query
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
