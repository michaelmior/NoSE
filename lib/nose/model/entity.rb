module NoSE
  # A representation of an object in the conceptual data model
  class Entity
    attr_reader :fields, :name
    attr_accessor :count

    def initialize(name, &block)
      @name = name
      @fields = {}
      @count = 1

      # Precompute the hash
      hash

      # Apply the DSL
      EntityDSL.new(self).instance_eval(&block) if block_given?
    end

    # :nocov:
    def to_color
      "[light_blue]#{@name}[/] [#{fields.keys.map(&:to_color).join ', '}]"
    end
    # :nocov:

    # Compare by name
    # @return [Boolean]
    def ==(other)
      @name == other.name
    end
    alias_method :eql?, :==

    # The hash is based on the name of the entity and its fields
    # @return [Fixnum]
    def hash
      @hash ||= Zlib.crc32 @name
    end

    # Get the key fields for the entity
    # @return [Array<Fields::Field>]
    def id_fields
      fields.values.select(&:primary_key?)
    end

    # Get all foreign key fields on the entity
    # @return [Array<Fields::ForeignKeyField>]
    def foreign_keys
      fields.values.select { |field| field.is_a? Fields::ForeignKeyField }
    end

    # Find the foreign key to a particular entity
    # @return [Fields::Field, nil]
    def foreign_key_for(entity)
      fields.values.find do |field|
        field.is_a?(Fields::ForeignKeyField) && field.entity == entity
      end
    end

    # Adds a {Fields::Field} to the entity
    def <<(field)
      @fields[field.name] = field
      field.instance_variable_set(:@parent, self)

      field.hash
      field.freeze

      self
    end

    # Shortcut for {#count=}
    # @return [Entity]
    def *(other)
      if other.is_a? Integer
        @count = other
      else
        fail TypeError, 'count must be an integer'
      end

      self
    end

    # Get the field on the entity with the given name
    # @return [Field]
    def [](field)
      fail FieldNotFound unless field? field
      @fields[field]
    end

    # Return true if the entity contains a field with the given name
    def field?(field)
      @fields.key? field
    end
  end

  # A helper class for DSL creation to avoid messing with {Entity}
  class EntityDSL
    def initialize(entity)
      @entity = entity
    end

    # rubocop:disable MethodName

    # Specify a list of field names for the primary key
    def PrimaryKey(*names)
      # Unset the old keys and set new ones,
      # we dup because the fields are frozen
      @entity.fields.values.each do |field|
        if field.primary_key?
          field = field.dup
          field.primary_key = false
          @entity.fields[field.name] = field
          field.freeze
        end
      end

      names.each do |name|
        field = @entity[name].dup
        field.primary_key = true
        @entity.fields[name] = field
        field.freeze
      end
    end

    # rubocop:enable MethodName

    def etc(size = 1)
      @entity << Fields::HashField.new('**', size)
    end
  end

  # Raised when looking up a field on an entity which does not exist
  class FieldNotFound < StandardError
  end
end
