# frozen_string_literal: true

module NoSE
  # A representation of an object in the conceptual data model
  class Entity
    attr_reader :fields
    attr_reader :foreign_keys, :name
    attr_accessor :count

    def initialize(name, &block)
      @name = name
      @fields = {}
      @foreign_keys = {}
      @count = 1

      # Precompute the hash
      hash

      # Apply the DSL
      EntityDSL.new(self).instance_eval(&block) if block_given?
    end

    # :nocov:
    # @return [String]
    def to_color
      "[light_blue]#{@name}[/] [#{fields.each_key.map(&:to_color).join ', '}]"
    end
    # :nocov:

    # Compare by name
    # @return [Boolean]
    def ==(other)
      @name == other.instance_variable_get(:@name)
    end
    alias eql? ==

    # The hash is based on the name of the entity and its fields
    # @return [Fixnum]
    def hash
      @hash ||= @name.hash
    end

    # Get the key fields for the entity
    # @return [Fields::IDField>]
    def id_field
      fields.each_value.find(&:primary_key?)
    end

    # Adds a {Fields::Field} to the entity
    # @return [self] the current entity to allow chaining
    def <<(field, freeze: true)
      if field.is_a? Fields::ForeignKeyField
        @foreign_keys[field.name] = field
      else
        @fields[field.name] = field
      end

      field.instance_variable_set(:@parent, self)
      field.hash
      field.freeze if freeze

      self
    end

    # Shortcut for {#count=}
    # @return [Entity]
    def *(other)
      fail TypeError, 'count must be an integer' unless other.is_a? Integer
      @count = other

      self
    end

    # Get the field on the entity with the given name
    # @return [Field]
    def [](field_name)
      field = @fields[field_name] || @foreign_keys[field_name]
      fail FieldNotFound if field.nil?
      field
    end

    # Return true if the entity contains a field with the given name
    def field?(field)
      @fields.key? field
    end

    # Generate a hash with random values for fields in the entity
    # @return [Hash]
    def random_entity(prefix_entity = true)
      Hash[@fields.map do |name, field|
        key = name
        key = "#{@name}_#{name}" if prefix_entity
        [key, field.random_value]
      end]
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
      @entity.fields.each_value do |field|
        next unless field.primary_key?
        field = field.dup
        field.primary_key = false
        @entity.fields[field.name] = field
        field.freeze
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
