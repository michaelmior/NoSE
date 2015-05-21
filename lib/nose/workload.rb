require_relative 'model'
require_relative 'parser'

require 'erb'
require 'graphviz'

module NoSE
  # A representation of a query workload over a given set of entities
  class Workload
    attr_reader :model, :statement_weights

    def initialize(model=nil, &block)
      @statement_weights = {}
      @model = model || Model.new
      @entities = {}

      # Apply the DSL
      WorkloadDSL.new(self).instance_eval(&block) if block_given?
    end

    # Add a new {Entity} or {Statement} to the workload
    def <<(other)
      if other.is_a? Entity
        @model.add_entity other.freeze
      elsif other.is_a? Statement
        add_statement other.freeze
      else
        fail TypeError, 'can only add queries and entities to a workload'
      end
    end

    # Add a new {Statement} to the workload or parse a string
    def add_statement(statement, weight = 1)
      statement = Statement.parse(statement, @model) if statement.is_a? String

      @statement_weights[statement.freeze] = weight
    end

    # Strip the weights from the query dictionary and return a list of queries
    # @return [Array<Statement>]
    def queries
      @statement_weights.keys.select { |statement| statement.is_a? Query }
    end

    # Strip the weights and return a list of statements
    # @return [Array<Statement>]
    def statements
      @statement_weights.keys
    end

    # Strip the weights from the query dictionary and return a list of updates
    # @return [Array<Statement>]
    def updates
      @statement_weights.keys.reject { |statement| statement.is_a? Query }
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
    def HasMany(from_name, to_name, entities, **options)
      from_entity, to_entity = entities.first
      field = Fields::ForeignKeyField.new from_name,
                                          @workload.model[to_entity],
                                          **options
      @workload.model[from_entity] << field

      # Add the key in the opposite direction
      options[:count] = @workload.model[from_entity].count
      options[:relationship] = :many
      field = Fields::ForeignKeyField.new to_name,
                                          @workload.model[from_entity],
                                          **options
      @workload.model[to_entity] << field
    end

    # Add a HasOne operation which is just the opposite of HasMany
    def HasOne(from_name, to_name, entities, **options)
      HasMany to_name, from_name, [entities.first.reverse].to_h, **options
    end

    # Shortcut to add a new {Statement} to the workload
    def Q(statement, weight = 1.0)
      @workload.add_statement statement, weight
    end

    # rubocop:enable MethodName
  end
end
