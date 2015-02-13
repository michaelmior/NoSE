require 'forwardable'
require 'zlib'

module NoSE::Fields
  # A single field on an {Entity}
  class Field
    include Supertype

    attr_reader :name, :size, :parent

    TYPE = nil

    def initialize(name, size, count: nil)
      @name = name
      @size = size
      @cardinality = count
    end

    # Compare by parent entity and name
    def ==(other)
      other.is_a?(Field) && @parent == other.parent &&
        @name == other.name
    end
    alias_method :eql?, :==

    # Hash by entity and name
    # @return [Fixnum]
    def hash
      @hash ||= Zlib.crc32 [@parent.name, @name].to_s
    end

    # :nocov:
    def to_color
      "[blue]#{parent.name}[/].[blue]#{name}[/]"
    end
    # :nocov:

    # A simple string representing the field
    def id
      "#{parent.name}_#{name}"
    end

    # Set the estimated cardinality of the field
    # @return [Field]
    def *(other)
      @cardinality = other
      self
    end

    # Return the previously set cardinality, falling back to the number of
    # entities for the field if set, or just 1
    def cardinality
      @cardinality || @parent.count || 1
    end

    # Check if this field is a foreign key to the given entity
    def foreign_key_to?(_entity)
      false
    end

    # @abstract Subclasses should produce a typed value from a string
    # :nocov:
    def self.value_from_string(_string)
      fail NotImplementedError
    end
    # :nocov:

    # Populate a helper DSL object with all subclasses of Field
    def self.inherited(child_class)
      # Add convenience methods for all field types for an entity DSL
      method_name = child_class.name.sub(/^NoSE::Fields::(.*?)(Field)?$/, '\1')
      NoSE::EntityDSL.send :define_method, method_name,
                           (proc do |*args|
                             send(:instance_variable_get, :@entity).send \
                                 :<<, child_class.new(*args)
                           end)

      child_class.send(:include, Subtype)
    end
    private_class_method :inherited
  end

  # Field holding an integer
  class IntegerField < Field
    TYPE = Integer

    def initialize(name, **options)
      super(name, 8, **options)
    end

    # Parse an Integer from the provided parameter
    def self.value_from_string(string)
      string.to_i
    end
  end

  # Field holding a float
  class FloatField < Field
    TYPE = Fixnum

    def initialize(name, **options)
      super(name, 8, **options)
    end

    # Parse a Float from the provided parameter
    def self.value_from_string(string)
      string.to_f
    end
  end

  # Field holding a string of some average length
  class StringField < Field
    TYPE = String

    def initialize(name, length = 10, **options)
      super(name, length, **options)
    end

    # Return the String parameter as-is
    def self.value_from_string(string)
      string
    end
  end

  # Field holding a date
  class DateField < Field
    def initialize(name, **options)
      super(name, 8, **options)
    end

    # Parse a DateTime from the provided parameter
    def self.value_from_string(string)
      DateTime.parse(string).to_time
    end
  end

  # Field representing a hash of multiple values
  class HashField < Field
    def initialize(name, size = 1, **options)
      super(name, size, **options)
    end
  end

  # Field holding a unique identifier
  class IDField < Field
    def initialize(name, **options)
      super(name, 16, **options)
    end

    # Return the String parameter as-is
    def self.value_from_string(string)
      string
    end
  end

  # Field holding a foreign key to another entity
  class ForeignKeyField < IDField
    attr_reader :entity, :relationship

    def initialize(name, entity, **options)
      super(name, **options)
      @relationship = :one

      # XXX: This is a hack which allows us to look up the stack to find an
      #      enclosing workload and the entity being referenced by the key
      if entity.is_a? String
        workload = NoSE::Workload.current
        entity = workload.model[entity] unless workload.nil?
      end
      @entity = entity
    end

    # The number of entities associated with the foreign key,
    # or a manually set cardinality
    def cardinality
      @entity.count || super
    end

    # Check if this field is a foreign key to the given entity
    def foreign_key_to?(entity)
      @entity == entity
    end
  end

  # Alias of {ForeignKeyField}
  ToOneKeyField = ForeignKeyField

  # Field holding a foreign key to many other entities
  class ToManyKeyField < ForeignKeyField
    def initialize(name, entity, **options)
      super(name, entity, **options)
      @relationship = :many
    end

    # An estimated number of entities on the other side of the relationship
    def cardinality
      # TODO: Properly calculation probably based on IndexLookupPlanStep#sample
      super
    end
  end

  # All the keys found when traversing foreign keys
  class KeyPath
    include Enumerable

    # Most of the work is delegated to the array
    extend Forwardable
    def_delegators :@key_fields, :each, :<<, :[], :==, :===, :eql?,
                   :inspect, :to_s, :to_a, :to_ary, :last

    def initialize(path, entity)
      @key_fields = fields_for_path path, entity
    end

    # Get the keys along a given path of entities
    # @return [Array<Field>]
    def fields_for_path(path, entity)
      path = path[1..-1] if path[0] == entity.name

      key_field = entity.fields[path[0]]
      if key_field.instance_of? IDField
        [key_field]
      elsif key_field.is_a? ForeignKeyField
        [key_field] + key_field.entity.key_fields(path[1..-1])
      else
        entity.id_fields.to_a
      end
    end

    # Get the common prefix of two paths
    # return [Array<Field>]
    def &(other)
      @key_fields.longest_common_prefix \
        other.instance_variable_get(:@key_fields)
    end
  end
end
