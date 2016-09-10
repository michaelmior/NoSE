# frozen_string_literal: true

require_relative 'model'
require_relative 'parser'

require 'erb'

module NoSE
  # A representation of a query workload over a given set of entities
  class Workload
    # The subdirectory workloads are loaded from
    LOAD_PATH = 'workloads'
    include Loader

    attr_reader :model
    attr_accessor :mix

    def initialize(model = nil, &block)
      @statement_weights = { default: {} }
      @model = model || Model.new
      @mix = :default

      # Apply the DSL
      WorkloadDSL.new(self).instance_eval(&block) if block_given?
    end

    # Compare models and statements
    # @return [Boolean]
    def ==(other)
      other.is_a?(Workload) && @model == other.model &&
        statement_weights == other.statement_weights
    end
    alias eql? ==

    # Add a new {Entity} or {Statement} to the workload
    # @return [self] the current workload to allow chaining
    def <<(other)
      if other.is_a? Entity
        @model.add_entity other.freeze
      elsif other.is_a? Statement
        add_statement other.freeze
      else
        fail TypeError, 'can only add queries and entities to a workload'
      end

      self
    end

    # Add a new {Statement} to the workload or parse a string
    # @return [void]
    def add_statement(statement, mixes = {}, group: nil, label: nil)
      statement = Statement.parse(statement, @model,
                                  group: group, label: label) \
        if statement.is_a? String
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
      (@statement_weights[@mix] || {}).keys
    end

    # Retrieve the weights for the current mix
    # @return [Hash]
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

    # Find a statement in the workload with the provided tag
    # @return [Statement]
    def find_with_tag(tag)
      statements.find do |s|
        s.text.end_with? "-- #{tag}"
      end
    end

    # Remove any updates from the workload
    # @return [void]
    def remove_updates
      @statement_weights[@mix].select! { |stmt, _| stmt.is_a? Query }
    end

    # Get all the support queries for updates in the workload
    # @return[Array<Statement>]
    def support_queries(indexes)
      updates.map do |update|
        indexes.map { |index| update.support_queries(index) }
      end.flatten(2)
    end

    # Check if all the fields used by queries in the workload exist
    # @return [Boolean]
    def fields_exist?
      @statement_weights[@mix].each_key do |query|
        # Projected fields and fields in the where clause exist
        fields = query.where.map(&:field) + query.fields
        fields.each do |field|
          return false unless @model.find_field field.value
        end
      end

      true
    end

    # Produce the source code used to define this workload
    # @return [String]
    def source_code
      return @source_code unless @source_code.nil?

      ns = OpenStruct.new(workload: self)
      tmpl = File.read File.join(File.dirname(__FILE__),
                                 '../../templates/workload.erb')
      tmpl = ERB.new(tmpl, nil, '>')
      @source_code = tmpl.result(ns.instance_eval { binding })
    end
  end

  # A helper class for DSL creation to avoid messing with {Workload}
  class WorkloadDSL
    def initialize(arg)
      if arg.is_a? Workload
        @workload = arg
        @model = arg.model
      elsif arg.is_a? Model
        @model = arg
      end
    end

    # rubocop:disable MethodName

    # Allow the use of an external model
    def Model(name)
      @workload.instance_variable_set(:@model, NoSE::Model.load(name))
    end

    # Shortcut to add a new {Entity} to the workload
    # @return [Entity]
    def Entity(*args, &block)
      @model.add_entity Entity.new(*args, &block)
    end

    # Add a HasMany relationship which is just the opposite of HasOne
    # @return [void]
    def HasMany(from_name, to_name, entities, **options)
      HasOne to_name, from_name, Hash[[entities.first.reverse]], **options
    end

    # Separate function for foreign keys to avoid circular dependencies
    # @return [void]
    def HasOne(from_name, to_name, entities, **options)
      from_entity, to_entity = entities.first
      from_field = Fields::ForeignKeyField.new from_name,
                                               @model[to_entity],
                                               **options

      # Add the key in the opposite direction
      options[:count] = @model[from_entity].count
      options[:relationship] = :many
      to_field = Fields::ForeignKeyField.new to_name,
                                             @model[from_entity],
                                             **options

      # Set the opposite keys and add to entities
      to_field.reverse = from_field
      from_field.reverse = to_field
      @model[from_entity] << from_field
      @model[to_entity] << to_field
    end

    # Shortcut to add a new {Statement} to the workload
    # @return [void]
    def Q(statement, weight = 1.0, group: nil, label: nil, **mixes)
      fail 'Statements require a workload' if @workload.nil?

      return if weight.zero? && mixes.empty?
      mixes = { default: weight } if mixes.empty?
      @workload.add_statement statement, mixes, group: group, label: label
    end

    # Allow setting the default workload mix
    # @return [void]
    def DefaultMix(mix)
      @workload.mix = mix
    end

    # Allow grouping statements with an associated weight
    # @return [void]
    def Group(name, weight = 1.0, **mixes, &block)
      fail 'Groups require a workload' if @workload.nil?

      # Apply the DSL
      dsl = GroupDSL.new
      dsl.instance_eval(&block) if block_given?
      dsl.statements.each do |statement|
        Q(statement, weight, **mixes, group: name)
      end
    end

    # rubocop:enable MethodName
  end

  # A helper class for DSL creation to allow groups of statements
  class GroupDSL
    attr_reader :statements

    def initialize
      @statements = []
    end

    # rubocop:disable MethodName

    # Track a new statement to be added
    # @return [void]
    def Q(statement)
      @statements << statement
    end

    # rubocop:enable MethodName
  end
end
