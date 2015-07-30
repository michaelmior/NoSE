# rubocop:disable Documentation

require 'date'
require 'formatador'
require 'parallel'
require 'pp'
require 'stringio'

# Reopen to add utility methods
module Enumerable
  # Enumerate all non-empty prefixes of the enumerable
  def prefixes
    Enumerator.new do |enum|
      prefix = []
      each do |elem|
        prefix = prefix.dup << elem
        enum.yield prefix
      end
    end
  end

  # Enumerate all partitionings of an enumerable
  def partitions
    Enumerator.new do |enum|
      1.upto(length - 1).map do |length|
        enum.yield partition.with_index { |_, i| i < length }
      end
    end
  end
end

# Extend with some convenience methods
class Array
  # Find the longest common prefix of two arrays
  def longest_common_prefix(other)
    fail TypeError unless other.is_a? Array
    (prefixes.to_a & other.prefixes.to_a).max_by(&:length) || []
  end
end

# Reopen to present as finite as with Float
class Integer
  def finite?
    true
  end
end

# Extend Object to print coloured output
class Object
  def inspect
    Formatador.parse(respond_to?(:to_color) ? to_color : to_s)
  end

  # Get a colored representation of the object
  # @return [String]
  def to_color
    to_s
  end
end

# Allow a supertype to look up a class given the
# name of a subtype inheriting from this class
module Supertype
  # Add class methods when this module is included
  def self.included(base)
    base.extend ClassMethods
  end

  # Add a single method to get a class given the subtype name
  module ClassMethods
    # Get the class given the name of a subtype
    def subtype_class(name)
      class_name = self.name.split('::')[0..-2]
      class_name << (name.split('_').map do |name_part|
        name_part = name_part[0].upcase + name_part[1..-1]
        name_part.sub 'Id', 'ID'
      end.join)
      class_name[-1] = class_name[-1] + self.name.split('::').last

      class_name.reduce(Object) do |mod, name_part|
        mod.const_get(name_part)
      end
    end
  end
end

# Allow subclasses to return a string representing of the
# class, minus a common suffix also used in the superclass
module Subtype
  # Add instance and class methods when this module is included
  def self.included(base)
    base.send :include, InstanceMethods
    base.extend ClassMethods
  end

  # Mirror the subtype method on class instances
  module InstanceMethods
    # A mirror of {Subtype::ClassMethods#subtype_name}
    def subtype_name(**args)
      self.class.subtype_name(**args)
    end
  end

  # Add a single method to retrieve the subtype name
  module ClassMethods
    # Get a unique string identify this subclass amongst sibling classes
    # @return String
    def subtype_name(name_case: :snake)
      super_name = name_array superclass
      self_name = name_array self
      self_name = self_name.reverse.drop_while do |part|
        super_name.include? part
      end.reverse

      if name_case == :snake
        name = self_name.join '_'
      elsif name_case == :camel
        name = self_name.map { |part| part[0].upcase + part[1..-1] }.join ''
        name.sub! 'Id', 'ID'
      end

      name
    end

    private

    # Convert camel case class names to an array
    # @return [Array<String>]
    def name_array(cls)
      cls.name.sub('ID', 'Id').split('::').last.split(/(?=[A-Z]+)/) \
        .map(&:downcase)
    end
  end
end

# Simple helper class to facilitate cardinality estimates
class Cardinality
  # Update the cardinality based on filtering implicit to the index
  def self.filter(cardinality, eq_filter, range_filter)
    filtered = (range_filter.nil? ? 1.0 : 0.1) * cardinality
    filtered *= eq_filter.map do |field|
      1.0 / field.cardinality
    end.inject(1.0, &:*)

    [1, filtered.round].max
  end
end

# Add a simple function for pretty printing strings
module Kernel
  private

  # Pretty print to a string
  def pp_s(*objs)
    s = StringIO.new
    objs.each { |obj| PP.pp(obj, s) }
    s.rewind
    s.read
  end

  module_function :pp_s
end

# Add simple convenience methods
class Object
  # Convert all the keys of a hash to symbols
  def deep_symbolize_keys
    return inject({}) do |memo, (k, v)|
      memo[k.to_sym] = v.deep_symbolize_keys
      memo
    end if self.is_a? Hash

    return inject([]) do |memo, v|
      memo << v.deep_symbolize_keys
      memo
    end if self.is_a? Array

    self
  end
end

# Extend the kernel to allow warning suppression
module Kernel
  # Allow the suppression of warnings for a block of code
  def suppress_warnings
    original_verbosity = $VERBOSE
    $VERBOSE = nil
    result = yield
    $VERBOSE = original_verbosity
    return result
  end
end

module NoSE
  module DSL
    # Add methods to the class which can be used to access entities and fields
    def mixin_fields(entities, cls)
      entities.each do |entity_name, entity|
        # Add fake entity object for the DSL
        fake = Object.new

        # Add a method named by the entity to allow field creation
        cls.send :define_method, entity_name.to_sym, (proc do
          metaclass = class << fake; self; end

          # Allow fields to be defined using [] access
          metaclass.send :define_method, :[] do |field_name|
            if field_name == '*'
              entity.fields.values
            else
              entity.fields[field_name] || entity.foreign_keys[field_name]
            end
          end

          # Define methods named for fields so things like 'user.id' work
          entity.fields.merge(entity.foreign_keys).each do |field_name, field|
            metaclass.send :define_method, field_name.to_sym, -> { field }
          end

          fake
        end)
      end
    end

    module_function :mixin_fields
  end
end

class Time
  # Convert to a DateTime instance
  # http://stackoverflow.com/a/279785/123695
  def to_datetime
    # Convert seconds + microseconds into a fractional number of seconds
    seconds = sec + Rational(usec, 10**6)

    # Convert a UTC offset measured in minutes to one measured in a
    # fraction of a day.
    offset = Rational(utc_offset, 60 * 60 * 24)
    DateTime.new(year, month, day, hour, min, seconds, offset)
  end
end

# rubocop:enable Documentation
