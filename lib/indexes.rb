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
      # TODO Support range queries
      return false
    else
      return false if not query.where.map { |condition|
          @fields.include?(workload.find_field condition.field.value) }.all?
    end

    true
  end
end
