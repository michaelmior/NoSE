require_relative 'model'
require_relative 'parser'

require 'erb'
require 'graphviz'

module NoSE
  # A representation of a query workload over a given set of entities
  class Workload
    attr_reader :model, :statement_weights
    thread_local_accessor :current

    def initialize(model=nil, &block)
      @statement_weights = {}
      @model = model || Model.new
      @entities = {}

      # Apply the DSL
      # XXX We use a hack here to track the enclosing workload
      #     which is used elsewhere to pretty up the DSL
      Workload.current = self
      WorkloadDSL.new(self).instance_eval(&block) if block_given?
      Workload.current = nil
    end

    # Add a new {Entity} or {Statement} to the workload
    def <<(other)
      if other.is_a? Entity
        @model.add_entity other.freeze
      elsif other.is_a? Statement
        add_query other.freeze
      else
        fail TypeError, 'can only add queries and entities to a workload'
      end
    end

    # Add a new {Statement} to the workload or parse a string
    def add_query(query, weight = 1)
      query = Statement.parse(query, @model) if query.is_a? String

      @statement_weights[query.freeze] = weight
    end

    # Strip the weights from the query dictionary and return a list of queries
    # @return [Array<Query>]
    def queries
      @statement_weights.keys.select { |statement| statement.is_a? Query }
    end

    # Strip the weights from the query dictionary and return a list of updates
    # @return [Array<Update>]
    def updates
      @statement_weights.keys.select { |statement| statement.is_a? Update }
    end

    # Check if all the fields used by queries in the workload exist
    # @return [Boolean]
    def fields_exist?
      @statement_weights.keys.each do |query|
        # Projected fields and fields in the where clause exist
        fields = query.where.map { |condition| condition.field } + query.fields
        fields.each do |field|
          return false unless @model.find_field field.value
        end
      end

      true
    end

    # Generate the identity maps for updates in the workload
    def identity_maps(indexes)
      # Get all the fields touched by updates
      updated_entities = Set.new
      updates.each do |update|
        update.settings.map(&:field).each do |field|
          updated_entities.add field.parent
        end
      end

      identity_maps = Set.new
      indexes.each do |index|
        # If no entities in the index path are updated, we don't need a map
        next unless updated_entities.any? do |entity|
          index.path.include? entity
        end

        # Loop over all entities in the index which are updated
        (updated_entities & index.path.to_set).each do |entity|
          # Get all the fields corresponding to this entity in the index
          update_fields = index.all_fields.select do |field|
            field.parent == entity
          end

          identity_maps.add Index.new entity.id_fields, [], update_fields,
                                      [entity]
        end
      end

      identity_maps
    end

    # Write the workload
    def output_rb(filename)
      ns = OpenStruct.new(workload: self)
      tmpl = File.read File.join(File.dirname(__FILE__), 'workload.erb')
      out = ERB.new(tmpl, nil, '>').result(ns.instance_eval { binding })
      File.open(filename, 'w') { |file| file.write out }
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
      @workload.model.add_entity Entity.new(*args, &block)
    end

    # Separate function for foreign keys to avoid circular dependencies
    def ForeignKey(name, parent, entity, count: nil)
      @workload[parent] << Fields::ForeignKeyField.new(name,
                                                       @workload.model[entity],
                                                       count: count)
    end

    # Shortcut to add a new {Statement} to the workload
    def Q(query, weight = 1.0)
      @workload.add_query query, weight
    end

    # rubocop:enable MethodName
  end
end
