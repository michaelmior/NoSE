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

# rubocop:enable Documentation
