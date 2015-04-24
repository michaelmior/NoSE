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
    def Q(statement, weight = 1.0)
      @workload.add_statement statement, weight
    end

    # rubocop:enable MethodName
  end
end
