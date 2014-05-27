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
    out = to_s
    $stdout.isatty ? out : out.uncolorize
  end
end

# rubocop:enable Documentation
