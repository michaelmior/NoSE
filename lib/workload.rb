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
    @entities[field[0]].fields[field[1]]
  end

  def valid?
    @queries.each do |query|
      # Entity must exist
      return false if not @entities.has_key?(query.from.value)
      entity = @entities[query.from.value]

      # All fields must exist
      query.fields.each do |field|
        return false if not entity.fields.has_key?(field.value)
      end

      # No more than one range query
      return false if query.where.count { |condition|
          [:>, :>=, :<, :<=].include?(condition.logical_operator.value) } > 1

      # Fields in the where clause exist
      query.where.map { |condition| condition.field }.each do |field|
        parts = field.value
        return false if not @entities.has_key?(parts.first)
        return false if parts.length == 2 and not entity.fields.has_key?(parts.last)
      end
    end

    true
  end
end
