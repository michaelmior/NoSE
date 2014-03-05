require_relative './model'
require_relative './parser'

class Workload
  attr_reader :queries
  attr_reader :entities

  def initialize
    @queries = []
    @entities = {}
  end

  def add_query(query)
    @queries << query
  end

  def add_entity(entity)
    @entities[entity.name] = entity
  end

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

  def get_entity(name)
    @entities[name]
  end

  def fields_exist?
    @queries.each do |query|
      entity = @entities[query.from.value]

      # All fields must exist
      query.fields.each do |field|
        return false unless entity.fields.key?(field.value)
      end

      # Fields in the where clause exist
      query.where.map { |condition| condition.field }.each do |field|
        parts = field.value
        return false unless @entities.key?(parts.first)
        return false if parts.length == 2 && \
          !entity.fields.key?(parts.last)
      end
    end
  end

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
