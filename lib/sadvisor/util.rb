# rubocop:disable Documentation

require 'formatador'
require 'parallel'

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

class Array
  def longest_common_prefix(other)
    fail TypeError unless other.is_a? Array
    (prefixes.to_a & other.prefixes.to_a).max_by(&:length) || []
  end
end

class Integer
  def finite?
    true
  end
end

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

module Supertype
  def self.included(base)
    base.extend ClassMethods
  end

  module ClassMethods
    # Get the class given the name of a subtype
    def subtype_class(name)
      class_name = self.name.split('::')[0..-2]
      class_name << (name.split('_').map do |name_part|
        name_part.capitalize.sub 'Id', 'ID'
      end.join)
      class_name[-1] = class_name[-1] + self.name.split('::').last

      class_name.reduce(Object) do |mod, name_part|
        mod.const_get(name_part)
      end
    end
  end
end

module Subtype
  def self.included(base)
    base.send :include, InstanceMethods
    base.extend ClassMethods
  end

  module InstanceMethods
    def subtype_name
      self.class.subtype_name
    end
  end

  module ClassMethods
    # Get a unique string identify this subclass amongst sibling classes
    # @return String
    def subtype_name
      super_name = name_array superclass
      self_name = name_array self
      self_name = self_name.reverse.drop_while do |part|
        super_name.include? part
      end.reverse

      self_name.join '_'
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

# rubocop:enable Documentation
