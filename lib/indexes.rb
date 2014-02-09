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
    (@fields + @extra).map(&:size).inject(:+) or 0
  end

  def size
    fields.map(&:cardinality).inject(1, :*) * self.entry_size or 0
  end
end
