require_relative 'model/entity'
require_relative 'model/fields'

module NoSE
  class Model
    attr_reader :entities

    def initialize
      @entities = {}
    end

    # Retrieve an entity by name
    # @return [Entity]
    def [](name)
      return @entities[name] if @entities.key? name
      fail EntityNotFound
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
        entity = field[0].is_a?(String) ? entities[field[0]] : field[0]
        entity[field[1]]
      end
    end

    # Output a PNG representation of entities in the model
    def output(format, filename, include_fields = false)
      graph = GraphViz.new :G, type: :digraph
      nodes = Hash[@entities.values.map do |entity|
        label = "#{entity.name}\n"
        if include_fields
          label += entity.fields.values.map do |field|
            type = field.class.name.sub(/^NoSE::(.*?)(Field)?$/, '\1')
            "#{field.name}: #{type}"
          end.join("\n")
        end

        [entity.name, graph.add_nodes(label)]
      end]

      entities.values.each do |entity|
        entity.foreign_keys.each do |key|
          graph.add_edges nodes[entity.name], nodes[key.entity.name]
        end
      end

      graph.output **{format => filename}
    end
  end

  # Raised when looking up an entity in the workload which does not exist
  class EntityNotFound < StandardError
  end
end
