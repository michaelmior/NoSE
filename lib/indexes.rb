require_relative 'node_extensions'

# A representation of materialized views over fields in an entity
class Index
  attr_reader :fields
  attr_reader :extra

  def initialize(fields, extra)
    @fields = fields

    # Track which key this field is mapped over
    @field_keys = {}
    @fields.each do |field|
      id_fields = field.parent.id_fields
      @field_keys[field] = id_fields ? id_fields[0..0] : []
    end

    @extra = extra
  end

  def inspect
    field_names = @fields.map(&:inspect)
    extra_names = @extra.map(&:inspect)
    (field_names.to_s + ', ' + extra_names.to_s).gsub '"', ''
  end

  def ==(other)
    @fields == other.fields && \
        @field_keys == other.instance_variable_get(:@field_keys) \
        && @extra == other.extra
  end

  # Set the keys which a field in the index is derived from
  # This is useful when a given entity may be reached via multiple foreign keys
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

  def contains_field?(field)
    !!(@fields + @extra).find { |index_field| field == index_field }
  end

  # The size of a single entry in the index
  def entry_size
    (@fields + @extra).map(&:size).inject(0, :+)
  end

  # The total size of this index
  def size
    fields.map(&:cardinality).inject(1, :*) * entry_size
  end

  # Check if the given predicates can be supported by this index
  def supports_predicates?(from, fields, eq, range, order_by, workload)
    # Ensure all the fields the query needs are indexed
    return false unless fields.map do |field|
      contains_field?(workload.find_field [from, field.value])
    end.all?

    # Track fields used in predicates
    predicate_fields = []

    # Range predicates must occur last
    if range
      range_field = workload.find_field range.field.value
      return false if @fields.last != range_field
      predicate_fields.push range.field.value
    end

    # All fields in the where clause must be indexes
    return false unless eq.map do |condition|
      @fields.include?(workload.find_field condition.field.value)
    end.all?
    predicate_fields += eq.map { |field| field.field.value }

    # Fields for ordering must appear last
    order_fields = order_by.map { |field| workload.find_field field }
    return false if order_fields.length != 0 && \
        order_fields != @fields[-order_fields.length..-1]
    predicate_fields += order_by

    from_entity = workload.entities[from]
    return false unless predicate_fields.map do |field|
      keys_for_field(workload.find_field field) == \
          from_entity.key_fields(field)
    end.all?

    true
  end

  # Check if an index can support a given query
  # @see #supports_predicates?
  def supports_query?(query, workload)
    supports_predicates? query.from.value, query.fields, query.eq_fields, \
                         query.range_field, query.order_by, workload
  end

  # The cost of evaluating a query for this index
  def query_cost(query, workload)
    # XXX This basically just calculates the size of the data fetched

    # Get all fields corresponding to equality predicates
    eq_fields = query.eq_fields.map do |condition|
      workload.find_field condition.field.value
    end

    # Estimate the number of results retrieved based on a uniform distribution
    cost = eq_fields.map do |field|
      field.cardinality * 1.0 / field.parent.count
    end.inject(workload.get_entity(query.from.value).count * 1.0, :*) \
        * entry_size

    # XXX Make a dumb guess that the selectivity of a range predicate is 1/3
    # see Query Optimization With One Parameter, Anjali V. Betawadkar, 1999
    cost *= 1.0 / 3 if query.range_field

    cost
  end
end

module CQL
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
        workload.find_field [from.value, field.value]
      end
      extra -= fields

      Index.new(fields, extra)
    end
  end
end

class Entity
  # Create a simple index which maps entity keys to other fields
  def simple_index
    Index.new(id_fields, fields.values - id_fields)
  end
end
