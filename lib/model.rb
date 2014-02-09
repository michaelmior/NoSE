class Entity
 attr_reader :fields
 attr_reader :name

 def initialize(name)
   @name = name
   @fields = {}
   @count = 1
 end

 def <<(field)
   @fields[field.name] = field
   field.instance_variable_set(:@parent, self)
   self
 end

 def *(count)
   @count = count
   self
 end
end

class Field
  attr_reader :name
  attr_reader :type
  attr_reader :size
  attr_reader :parent
  attr_reader :cardinality

  def initialize(name, type, size)
    @name = name
    @type = type
    @size = size
    @cardinality = 1
  end

  def *(cardinality)
    @cardinality = cardinality
    self
  end
end

class IntegerField < Field
  def initialize(name)
    super(name, :integer, 8)
  end
end

class FloatField < Field
  def initialize(name)
    super(name, :float, 8)
  end
end

class StringField < Field
  def initialize(name, length)
    super(name, :string, length)
  end
end


class IDField < Field
  def initialize(name)
    super(name, :key, 16)
  end
end

class ForeignKey < IDField
  attr_reader :entity
  attr_reader :cardinality

  def initialize(name, entity)
    super(name)
    @cardinality = :one
    @entity = entity
  end
end

ToOneKey = ForeignKey

class ToManyKey < ForeignKey
  def initialize(name, entity)
    super(name, entity)
    @cardinality = :many
  end
end
