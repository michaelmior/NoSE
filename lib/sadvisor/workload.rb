require_relative './model'
require_relative './parser'

module Sadvisor
  # A representation of a query workload over a given set of entities
  class Workload
    attr_reader :entities

    def initialize(&block)
      @query_weights = {}
      @entities = {}

      # Apply the DSL
      WorkloadDSL.new(self).instance_eval(&block) if block_given?
    end

    # Add a new {Entity} or {CQL::Statement} to the workload
    def <<(other)
      if other.is_a? Entity
        add_entity other
      elsif other.is_a? CQL::Statement
        add_query other
      else
        fail TypeError, 'can only add queries and entities to a workload'
      end
    end

    # Retrieve an entity by name
    # @return [Entity]
    def [](name)
      @entities[name]
    end

    # Add a new {CQL::Statement} to the workload or parse a string
    def add_query(query, weight = 1)
      query = Parser.parse query if query.is_a? String

      @query_weights[query] = weight
    end

    # Strip the weights from the query dictionary and return a list of queries
    # @return [Array<CQL::Statement>]
    def queries
      @query_weights.keys
    end

    # Add an {Entity} to the workload
    def add_entity(entity)
      @entities[entity.name] = entity
    end

    # Find a field given an +Enumerable+ of identifiers
    # @return [Field]
    def find_field(field)
      if field.count > 2
        # Do a foreign key lookup
        field = field.dup
        key_field = @entities[field[0]].fields[field[1]]
        field[0..1] = key_field ? key_field.entity.name : field[1]
        find_field field
      else
        @entities[field[0]].fields[field[1]]
      end
    end

    # Find the keys traversed looking up a given field
    def find_field_keys(field)
      find_field_keys_each field[0..-2].reverse
    end

    # Check if all the fields used by queries in the workload exist
    # @return [Boolean]
    def fields_exist?
      @query_weights.keys.each do |query|
        # Projected fields and fields in the where clause exist
        fields = query.where.map { |condition| condition.field } + query.fields
        fields.each do |field|
          return false unless find_field field.value
        end
      end
    end

    # Check if the queries are valid for the loaded entities
    # @return [Boolean]
    def valid?
      @query_weights.keys.each do |query|
        # Entity must exist
        return false unless @entities.key?(query.from.value)

        # No more than one range query
        return false if query.range_field

        return false unless valid_paths? query
      end

      fields_exist?
    end

    private

    # Iterative helper for {#find_field_keys}
    def find_field_keys_each(field, keys = [])
      if field.count >= 2
        field = field.dup
        key_field = @entities[field[0]].fields[field[1]]
        keys << (key_field ? [key_field] : @entities[field[0]].id_fields)
        field[0..1] = key_field ? key_field.entity.name : field[1]
        keys += find_field_keys_each(field)
        keys
      else
        [@entities[field[0]].id_fields]
      end
    end

    # Check if fields referenced by queries in the workload consist of valid
    # paths through the entity graph
    def valid_paths?(query)
      fields = query.where.map { |condition| condition.field }
      fields += query.order_by
      fields.map!(&:value)

      return true if fields.empty?

      longest = fields.max_by(&:count)
      fields.map { |field| longest[0..field.count - 1] == field }.all?
    end
  end

  private

  # A helper class for DSL creation to avoid messing with {Workload}
  class WorkloadDSL
    def initialize(workload)
      @workload = workload
    end

    # rubocop:disable MethodName

    # Shortcut to add a new {Entity} to the workload
    def Entity(*args, &block)
      @workload.add_entity Entity.new(*args, &block)
    end

    # Shortcut to add a new {CQL::Statement} to the workload
    def Q(query, weight)
      @workload.add_query query, weight
    end

    # rubocop:enable MethodName
  end
end
