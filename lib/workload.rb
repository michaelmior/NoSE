require_relative './model'
require_relative './parser'

# A representation of a query workload over a given set of entities
class Workload
  attr_reader :queries
  attr_reader :entities

  def initialize
    @queries = []
    @entities = {}
  end

  # Add a new {CQL::Statement} to the workload
  def add_query(query)
    @queries << query
  end

  # Add an {Entity} to the workload
  def add_entity(entity)
    @entities[entity.name] = entity
  end

  # Find a field given an +Enumerable+ of identifiers
  def find_field(field)
    if field.count > 2
      # Do a foreign key lookup
      field = field.dup
      field[0..1] = @entities[field[0]].fields[field[1]].entity.name
      find_field field
    else
      @entities[field[0]].fields[field[1]]
    end
  end

  # Retrieve an entity by name
  def get_entity(name)
    @entities[name]
  end

  # Check if all the fields used by queries in the workload exist
  def fields_exist?
    @queries.each do |query|
      entity = @entities[query.from.value]

      # Projected fields and fields in the where clause exist
      fields = query.where.map { |condition| condition.field } + query.fields
      fields.each do |field|
        parts = field.value
        return false unless @entities.key?(parts.first)
        return false if parts.length == 2 && \
          !entity.fields.key?(parts.last)
      end
    end
  end

  # Check if the queries are valid for the loaded entities
  def valid?
    @queries.each do |query|
      # Entity must exist
      return false unless @entities.key?(query.from.value)

      # No more than one range query
      return false if query.range_field

    end

    fields_exist?
  end
end
