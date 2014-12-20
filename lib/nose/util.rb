# rubocop:disable Documentation

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

# Reopen class to add thread local access
class Class
  # Allow thread local class variables
  def thread_local_accessor(name, options = {})
    m = Module.new
    m.module_eval do
      class_variable_set :"@@#{name}", Hash.new do
        |h, k| h[k] = options[:default]
      end
    end
    m.module_eval %{
      FINALIZER = lambda {|id| @@#{name}.delete id }

      def #{name}
        @@#{name}[Thread.current.object_id]
      end

      def #{name}=(val)
        ObjectSpace.define_finalizer Thread.current, FINALIZER \
          unless @@#{name}.has_key? Thread.current.object_id
        @@#{name}[Thread.current.object_id] = val
      end
    }

    class_eval do
      include m
      extend m
    end
  end
end

# Simple helper class to facilitate cardinality estimates
class Cardinality
  # Update the cardinality after traversing the index
  def self.new_cardinality(cardinality, eq_filter, range_filter, path)
    eq_filter = eq_filter.group_by(&:parent)
    path = path.reverse

    # Update cardinality via predicates for first (last) entity in path
    cardinality *= filter_cardinality eq_filter, range_filter, path.first

    path.each_cons(2) do |entity, next_entity|
      tail = entity.foreign_key_for(next_entity).nil?
      if tail
        cardinality = cardinality * 1.0 * next_entity.count / entity.count
      else
        cardinality = sample cardinality, next_entity.count
      end

      # Update cardinality via the filtering implicit to the index
      cardinality *= filter_cardinality eq_filter, range_filter, next_entity
    end

    [1, cardinality.ceil].max
  end

  # Update the cardinality based on filtering implicit to the index
  def self.filter_cardinality(eq_filter, range_filter, entity)
    filter = range_filter && range_filter.parent == entity ? 0.1 : 1.0
    filter *= (eq_filter[entity] || []).map do |field|
      1.0 / field.cardinality
    end.inject(1.0, &:*)

    filter
  end

  # Get the estimated cardinality of the set of samples of m items with
  # replacement from a set of cardinality n
  def self.sample(m, n)
    # http://math.stackexchange.com/a/32816/130124
    n * (1 - (1 - (1.0 / n))**m)
  end
end

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

# rubocop:enable Documentation
