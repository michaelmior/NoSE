# frozen_string_literal: true

require 'date'
require 'faker'
require 'forwardable'
require 'zlib'

module NoSE
  # Fields attached to each entity in the entity graph model
  module Fields
    # A single field on an {Entity}
    class Field
      include Supertype

      attr_reader :name, :size, :parent
      attr_accessor :primary_key
      alias primary_key? primary_key

      # The Ruby type of values stored in this field
      TYPE = nil

      def initialize(name, size, count: nil)
        @name = name
        @size = size
        @cardinality = count
        @primary_key = false
      end

      # Compare by parent entity and name
      def ==(other)
        other.is_a?(Field) && @parent == other.parent &&
          @name == other.name
      end
      alias eql? ==

      # Hash by entity and name
      # @return [Fixnum]
      def hash
        @hash ||= id.hash
      end

      # :nocov:
      def to_color
        "[blue]#{@parent.name}[/].[blue]#{@name}[/]"
      end
      # :nocov:

      # :nocov:
      def to_s
        "#{@parent.name}.#{@name}"
      end
      # :nocov:

      # A simple string representing the field
      def id
        @id ||= "#{@parent.name}_#{@name}"
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

      # @abstract Subclasses should produce a typed value from a string
      # :nocov:
      def self.value_from_string(_string)
        fail NotImplementedError
      end
      # :nocov:

      # @abstract Subclasses should produce a random value of the correct type
      # :nocov:
      def random_value
        fail NotImplementedError
      end
      # :nocov:

      # Populate a helper DSL object with all subclasses of Field
      def self.inherited(child_class)
        # We use separate methods for foreign keys
        begin
          fk_class = Fields.const_get('ForeignKeyField')
        rescue NameError
          fk_class = nil
        end
        return if !fk_class.nil? && child_class <= fk_class

        add_field_method(child_class)
        child_class.send(:include, Subtype)
      end
      private_class_method :inherited

      # Add convenience methods for all field types for an entity DSL
      def self.add_field_method(child_class)
        method_regex = /^NoSE::Fields::(.*?)(Field)?$/
        method_name = child_class.name.sub(method_regex, '\1')
        EntityDSL.send :define_method, method_name,
                       (proc do |*args|
                         send(:instance_variable_get, :@entity).send \
                           :<<, child_class.new(*args)
                       end)
      end
      private_class_method :add_field_method
    end

    # Field holding an integer
    class IntegerField < Field
      # Integers are stored as integers
      TYPE = Integer

      def initialize(name, **options)
        super(name, 8, **options)
        @cardinality = 10
      end

      # Parse an Integer from the provided parameter
      # @return [Fixnum]
      def self.value_from_string(string)
        string.to_i
      end

      # Random numbers up to the given size
      # @return [Fixnum]
      def random_value
        rand(@cardinality)
      end
    end

    # Field holding a boolean value
    class BooleanField < Field
      # Since Ruby has no boolean type, we use Object
      # but all values will be either false or true
      TYPE = Object

      def initialize(name, **options)
        super(name, 1, **options)
        @cardinality = 2
      end

      # Check for strings true or false otherwise assume integer
      # @return [Boolean]
      def self.value_from_string(string)
        string = string.downcase
        if string[0] == 't'
          return true
        elsif string[0] == 'f'
          return false
        else
          [false, true][string.to_i]
        end
      end

      # Randomly true or false
      # @return [Boolean]
      def random_value
        [false, true][rand(2)]
      end
    end

    # Field holding a float
    class FloatField < Field
      # Any Fixnum is a valid float
      TYPE = Fixnum

      def initialize(name, **options)
        super(name, 8, **options)
      end

      # Parse a Float from the provided parameter
      def self.value_from_string(string)
        string.to_f
      end

      # Random numbers up to the given size
      def random_value
        rand(@cardinality).to_f
      end
    end

    # Field holding a string of some average length
    class StringField < Field
      # Strings are stored as strings
      TYPE = String

      def initialize(name, length = 10, **options)
        super(name, length, **options)
      end

      # Return the String parameter as-is
      # @return [String]
      def self.value_from_string(string)
        string
      end

      # A random string of the correct length
      # @return [String]
      def random_value
        Faker::Lorem.characters(@size)
      end
    end

    # Field holding a date
    class DateField < Field
      # Time is used to store timestamps
      TYPE = Time

      def initialize(name, **options)
        super(name, 8, **options)
      end

      # Parse a DateTime from the provided parameter
      # @return [Time]
      def self.value_from_string(string)
        # rubocop:disable Style/RedundantBegin
        begin
          DateTime.parse(string).to_time
        rescue ArgumentError
          raise TypeError
        end
        # rubocop:enable Style/RedundantBegin
      end

      # A random date within 2 years surrounding today
      # @return [Time]
      def random_value
        prev_year = DateTime.now.prev_year
        prev_year = prev_year.new_offset(Rational(0, 24))

        next_year = DateTime.now.next_year
        next_year = next_year.new_offset(Rational(0, 24))

        Faker::Time.between prev_year, next_year
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
      alias entity parent

      def initialize(name, **options)
        super(name, 16, **options)
        @primary_key = true
      end

      # Return the String parameter as-is
      # @return [String]
      def self.value_from_string(string)
        string
      end

      # nil value which is interpreted by the backend as requesting a new ID
      # @return [nil]
      def random_value
        nil
      end
    end

    # Field holding a foreign key to another entity
    class ForeignKeyField < IDField
      attr_reader :entity, :relationship
      attr_accessor :reverse

      def initialize(name, entity, **options)
        @relationship = options.delete(:relationship) || :one
        super(name, **options)
        @primary_key = false
        @entity = entity
      end

      # The number of entities associated with the foreign key,
      # or a manually set cardinality
      # @return [Fixnum]
      def cardinality
        @entity.count || super
      end
    end
  end
end
