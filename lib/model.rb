class Entity
 attr_reader :fields
 attr_reader :name

 def initialize(name)
   @name = name
   @fields = {}
 end

 def <<(field)
   @fields[field.name] = field
   field.instance_variable_set(:@parent, self)
   self
 end
end

class Field
  attr_reader :name
  attr_reader :type
  attr_reader :size
  attr_reader :parent
end

class IntegerField < Field
  def initialize(name)
    @name = name
    @type = :integer
    @size = 8
  end
end

class FloatField < Field
  def initialize(name)
    @name = name
    @type = :float
    @size = 8
  end
end

class StringField < Field
  def initialize(name, length)
    @name = name
    @type = :string
    @size = length
  end
end


class IDField < Field
  def initialize(name)
    @name = name
    @type = :key
    @size = 16
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
