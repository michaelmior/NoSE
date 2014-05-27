# rubocop:disable Documentation

require 'colorize'

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

  def to_color
    to_s
  end
end

# rubocop:enable Documentation
