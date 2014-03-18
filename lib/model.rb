class Entity
  attr_reader :fields
  attr_reader :name
  attr_reader :count

  def initialize(name)
    @name = name
    @fields = {}
    @count = 1
  end

  def id_fields
    fields.values.select { |field| field.instance_of? IDField }
  end

  def <<(field)
    @fields[field.name] = field
    field.instance_variable_set(:@parent, self)
    self
  end

  def *(other)
    if other.is_a? Integer
      @count = other
    else
      fail TypeError
    end

    self
  end

  def key_fields(field)
    field = field[1..-1] if field[0] == name

    key_field = @fields[field[0]]
    if key_field.instance_of? IDField
      [key_field]
    elsif key_field.is_a? ForeignKey
      [key_field] + key_field.entity.key_fields(field[1..-1])
    else
      id_fields
    end
  end
end

class Field
  attr_reader :name
  attr_reader :type
  attr_reader :size
  attr_reader :parent

  def initialize(name, type, size)
    @name = name
    @type = type
    @size = size
    @cardinality = nil
  end

  def inspect
    name + '.' + parent.name
  end

  def *(other)
    if other.is_a? Integer
      @cardinality = other
    else
      fail TypeError
    end

    self
  end

  def cardinality
    @cardinality || @parent.count || 1
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
  attr_reader :relationship

  def initialize(name, entity)
    super(name)
    @relationship = :one
    @entity = entity
  end

  def cardinality
    @entity.count || super
  end
end

ToOneKey = ForeignKey

class ToManyKey < ForeignKey
  def initialize(name, entity)
    super(name, entity)
    @relationship = :many
  end
end
