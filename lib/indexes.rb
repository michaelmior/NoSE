require_relative 'node_extensions'


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

  def set_field_keys(field, keys)
    @field_keys[field] = keys
  end

  def keys_for_field(field)
    @field_keys[field]
  end

  def identity_for?(entity)
    @fields == entity.id_fields
  end

  def has_field?(field)
    !!(@fields + @extra).detect { |index_field| field == index_field }
  end

  def entry_size
    (@fields + @extra).map(&:size).inject(0, :+)
  end

  def size
    fields.map(&:cardinality).inject(1, :*) * self.entry_size
  end

  def supports_query?(query, workload)
    # Ensure all the fields the query needs are indexed
    return false if not query.fields.map { |field|
        self.has_field?(workload.find_field [query.from.value, field.value]) }.all?

    # Track fields used in predicates
    predicate_fields = []

    # Check if the query contains a range predicate
    range = query.range_field

    # Range predicates must occur last
    if range
      range_field = workload.find_field range.field.value
      return false if @fields.last != range_field
      predicate_fields.push range.field.value
    end

    # All fields in the where clause must be indexes
    return false if not query.where.map { |condition|
         @fields.include?(workload.find_field condition.field.value) }.all?
    predicate_fields += query.where.map { |field| field.field.value }

    # Fields for ordering must appear last
    order_by = query.order_by.map { |field| workload.find_field field }
    return false if order_by.length != 0 and not order_by == @fields[-order_by.length..-1]
    predicate_fields += query.order_by

    from_entity = workload.entities[query.from.value]
    return false if not predicate_fields.map do |field|
      field_keys = self.keys_for_field workload.find_field(field)
      field_keys == from_entity.key_fields(field)
    end.all?

    true
  end

  def query_cost(query, workload)
    # XXX This basically just calculates the size of the data fetched

    # Get all fields corresponding to equality predicates
    eq_fields = query.eq_fields.map { |condition|
            workload.find_field condition.field.value }

    # Estimate the number of results retrieved based on a uniform distribution
    cost = eq_fields.map { |field|
        field.cardinality * 1.0 / field.parent.count } \
            .inject(workload.get_entity(query.from.value).count * 1.0, :*) * self.entry_size

    # XXX Make a dumb guess that the selectivity of a range predicate is 1/3
    # see Query Optimization With One Parameter, Anjali V. Betawadkar, 1999
    if query.range_field
      cost *= 1.0/3
    end

    cost
  end
end

module CQL
  class Statement
    def materialize_view(workload)
      fields = self.eq_fields
      if self.range_field
        fields.push self.range_field
      end
      fields = fields.map { |field| workload.find_field field.field.value }
      fields += self.order_by.map { |field| workload.find_field field }

      extra = self.fields.map { |field| workload.find_field [self.from.value, field.value] }
      extra -= fields
      Index.new(fields, extra)
    end
  end
end

class Entity
  def simple_index
    Index.new(self.id_fields, self.fields.values - self.id_fields)
  end
end
