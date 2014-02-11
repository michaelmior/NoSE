class Index
  attr_reader :fields

  def initialize(fields, extra)
    @fields = fields
    @extra = extra
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

    # Check if the query contains a range predicate
    range = query.where.detect { |condition| condition.is_range? }

    if range
      eq_fields = @fields[0..(@fields.index(workload.find_field range.field.value))]
    else
      eq_fields = @fields
    end

    return false if not query.where.map { |condition|
         eq_fields.include?(workload.find_field condition.field.value) }.all?

    true
  end

  def query_cost(query, workload)
    # XXX This basically just calculates the size of the data fetched

    cost = 0

    # Get all fields corresponding to equality predicates
    eq_fields = query.where.select { |condition|
        not condition.is_range? }.map { |condition|
            workload.find_field condition.field.value }

    # Estimate the number of results retrieved based on a uniform distribution
    cost += eq_fields.map { |field|
        field.cardinality * 1.0 / field.parent.count } \
            .inject(workload.get_entity(query.from.value).count * 1.0, :*) * self.entry_size

    # TODO Add range queries (need selectivity estimate)

    cost
  end
end
