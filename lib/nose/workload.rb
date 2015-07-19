require_relative 'model'
require_relative 'parser'

require 'erb'
require 'graphviz'

module NoSE
  # A representation of a query workload over a given set of entities
  class Workload
    attr_reader :model
    attr_accessor :mix

    def initialize(model = nil, &block)
      @statement_weights = { default: Hash.new { |*| 1 } }
      @model = model || Model.new
      @entities = {}
      @mix = :default

      # Apply the DSL
      WorkloadDSL.new(self).instance_eval(&block) if block_given?
    end

    # Adjust the percentage of writes in the workload
    def scale_writes(scale)
      @statement_weights.values.each do |weights|
        # Calculate the divisors for reads and writes
        read_total = weights.to_a.reduce 0 do |sum, (stmt, weight)|
          sum + (stmt.is_a?(Query) ? weight : 0)
        end
        write_total = weights.values.inject(0, &:+) - read_total
        read_scale = (read_total / (read_total + write_total)) / (1.0 - scale)
        write_scale = (write_total / (read_total + write_total)) / scale

        # Scale each of the weights by the calculated factor
        weights.keys.each do |stmt|
          weights[stmt] /= stmt.is_a?(Query) ? read_scale : write_scale
        end
      end
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
    def add_statement(statement, mixes = {})
      statement = Statement.parse(statement, @model) if statement.is_a? String
      statement.freeze

      mixes = { default: mixes } if mixes.is_a? Numeric
      mixes = { default: 1.0 } if mixes.empty?
      mixes.each do |mix, weight|
        @statement_weights[mix] = {} unless @statement_weights.key? mix
        @statement_weights[mix][statement] = weight
      end
    end

    # Strip the weights from the query dictionary and return a list of queries
    # @return [Array<Statement>]
    def queries
      @statement_weights[@mix].keys.select do |statement|
        statement.is_a? Query
      end
    end

    # Strip the weights and return a list of statements
    # @return [Array<Statement>]
    def statements
      @statement_weights[@mix].keys
    end

    # Retrieve the weights for the current mix
    def statement_weights
      @statement_weights[@mix]
    end

    # Strip the weights from the query dictionary and return a list of updates
    # @return [Array<Statement>]
    def updates
      @statement_weights[@mix].keys.reject do |statement|
        statement.is_a? Query
      end
    end

    # Remove any updates from the workload
    def remove_updates
      @statement_weights[@mix].select! { |stmt, _| stmt.is_a? Query }
    end

    # Get all the support queries for updates in the workload
    def support_queries(indexes)
      updates.map do |update|
        indexes.map { |index| update.support_queries(index) }
      end.flatten(2)
    end

    # Check if all the fields used by queries in the workload exist
    # @return [Boolean]
    def fields_exist?
      @statement_weights[@mix].keys.each do |query|
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
      from_field = Fields::ForeignKeyField.new from_name,
                                               @workload.model[to_entity],
                                               **options

      # Add the key in the opposite direction
      options[:count] = @workload.model[from_entity].count
      options[:relationship] = :many
      to_field = Fields::ForeignKeyField.new to_name,
                                             @workload.model[from_entity],
                                             **options

      # Set the opposite keys and add to entities
      to_field.reverse = from_field
      from_field.reverse = to_field
      @workload.model[from_entity] << from_field
      @workload.model[to_entity] << to_field
    end

    # Add a HasOne operation which is just the opposite of HasMany
    def HasOne(from_name, to_name, entities, **options)
      HasMany to_name, from_name, Hash[[entities.first.reverse]], **options
    end

    # Shortcut to add a new {Statement} to the workload
    def Q(statement, weight = 1.0, **mixes)
      return if weight == 0 && mixes.empty?
      mixes = { default: weight } if mixes.empty?
      @workload.add_statement statement, mixes
    end

    # rubocop:enable MethodName
  end
end
