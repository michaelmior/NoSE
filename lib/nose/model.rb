# frozen_string_literal: true

require_relative 'model/entity'
require_relative 'model/fields'

require 'graphviz'

module NoSE
  # A conceptual data model of a set of entities
  class Model
    # The subdirectory models are loaded from
    LOAD_PATH = 'models'
    include Loader

    attr_reader :entities

    def initialize(&block)
      @entities = {}

      # Apply the DSL
      WorkloadDSL.new(self).instance_eval(&block) if block_given?
    end

    # Compare all entities
    # @return [Boolean]
    def ==(other)
      other.is_a?(Model) && @entities = other.entities
    end
    alias eql? ==

    # Retrieve an entity by name
    # @return [Entity]
    def [](name)
      return @entities[name] if @entities.key? name
      fail EntityNotFound
    end

    # Add an {Entity} to the workload
    # @return [Entity]
    def add_entity(entity)
      fail InvalidEntity, 'no primary key defined' if entity.id_field.nil?
      @entities[entity.name] = entity
    end

    # Find a field given an +Enumerable+ of identifiers
    # @return [Field]
    def find_field(field)
      if field.count > 2
        find_field_chain field
      else
        find_entity_field(*field)
      end
    end

    # Output a PNG representation of entities in the model
    def output(format, filename, include_fields = false)
      graph = GraphViz.new :G, type: :digraph
      nodes = add_graph_nodes graph, include_fields
      add_graph_edges graph, nodes

      graph.output(**{ format => filename })
    end

    private

    # Add the nodes (entities) to a GraphViz object
    def add_graph_nodes(graph, include_fields)
      Hash[@entities.each_value.map do |entity|
        label = "#{entity.name}\n"
        if include_fields
          label += entity.fields.each_value.map do |field|
            type = field.class.name.sub(/^NoSE::(.*?)(Field)?$/, '\1')
            "#{field.name}: #{type}"
          end.join("\n")
        end

        [entity.name, graph.add_nodes(label)]
      end]
    end

    # Add the edges (foreign keys) to a GraphViz object
    def add_graph_edges(graph, nodes)
      @entities.each_value do |entity|
        entity.foreign_keys.each_value do |key|
          graph.add_edges nodes[entity.name], nodes[key.entity.name]
        end
      end
    end

    # Find a field in an entity where the entity may be a string or an object
    def find_field_chain(field)
      # Do a foreign key lookup
      field = field.dup
      key_field = @entities[field[0]][field[1]]
      field[0..1] = key_field ? key_field.entity.name : field[1]
      find_field field
    end

    # Find a field in an entity where the entity may be a string or an object
    def find_entity_field(entity, field)
      entity = entities[entity] if entity.is_a?(String)
      entity[field]
    end
  end

  # Raised when looking up an entity in the workload which does not exist
  class EntityNotFound < StandardError
  end

  # Raised when attempting to add an invalid entity to a workload
  class InvalidEntity < StandardError
  end
end
