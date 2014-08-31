require 'binding_of_caller'

module Sadvisor
  # A representation of an object in the conceptual data model
  class Entity
    attr_reader :fields, :name
    attr_accessor :count

    alias_method :state, :name

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
      @name.light_blue + ' [' + fields.keys.map(&:to_color).join(', ') + ']'
    end
    # :nocov:

    # Compare by name, fields, and count
    # @return [Boolean]
    def ==(other)
      hash == other.hash
    end
    alias_method :eql?, :==

    # The hash is based on the name of the entity and its fields
    # @return [Fixnum]
    def hash
      @hash ||= @name.hash
    end

    # Get the key fields for the entity
    # @return [Array<Field>]
    def id_fields
      fields.values.select { |field| field.instance_of? IDField }
    end

    # Get all foreign key fields on the entity
    # @return [Array<ForeignKey>]
    def foreign_keys
      fields.values.select { |field| field.is_a? ForeignKey }
    end

    # Find the foreign key to a particular entity
    # @return [Field, nil]
    def foreign_key_for(entity)
      fields.values.find do |field|
        field.is_a?(ForeignKey) && field.entity == entity
      end
    end

    # Adds a {Field} to the entity
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
        fail TypeError 'count must be an integer'
      end

      self
    end

    # Get the field on the entity with the given name
    # @return [Field]
    def [](field, silent = false)
      return @fields[field] if @fields.key? field
      fail FieldNotFound unless silent
    end

    # All the keys found when traversing foreign keys
    # @return [KeyPath]
    def key_fields(path)
      KeyPath.new path, self
    end
  end

  # A helper class for DSL creation to avoid messing with {Entity}
  class EntityDSL
    def initialize(entity)
      @entity = entity
    end
  end

  # Raised when looking up a field on an entity which does not exist
  class FieldNotFound < StandardError
  end
end
