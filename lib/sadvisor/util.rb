# rubocop:disable Documentation

require 'colorize'
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

class Fixnum
  def finite?
    true
  end
end

class Object
  def inspect
    out = respond_to?(:to_color) ? to_color : to_s
    $stdout.isatty ? out : out.uncolorize
  end

  # Get a colored representation of the object
  # @return [String]
  def to_color
    to_s
  end
end

#
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
