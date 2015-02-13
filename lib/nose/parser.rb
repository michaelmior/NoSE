require 'parslet'

# Parslet DSL extension for capturing the input source
class CaptureSource < Parslet::Atoms::Capture
  # Ugly hack to capture the source string that was parsed
  def apply(source, context, consume_all)
    before = source.instance_variable_get(:@str).rest
    success, value = result = super(source, context, consume_all)
    if success
      # Save the portion of the source string
      after = source.instance_variable_get(:@str).rest
      source_str = before[0..(before.length - after.length - 1)]
      value[(name.to_s + '_source').to_sym] = source_str
    end

    result
  end
end

# Extend the DSL to support capturing the source
module Parslet::Atoms::DSL
  # Capture some output along with the source string
  def capture_source(name)
    CaptureSource.new(self, name)
  end
end

# rubocop:disable Style/Blocks, Style/BlockEndNewline

module NoSE
  # Literals used in queries and updates
  module Literals
    include Parslet

    rule(:integer)     { match('[0-9]').repeat(1).as(:int) }
    rule(:quote)       { str('"') }
    rule(:nonquote)    { quote.absent? >> any }
    rule(:string)      { quote >> nonquote.repeat(1).as(:str) >> quote }
    rule(:literal)     { integer | string }
  end

  # Predicates used in queries and updates
  module Predicates
    include Parslet

    rule(:operator)    {
      str('=') | str('!=') | str('<=') | str('>=') | str('<') | str('>') }
    rule(:condition)   {
      field.as(:field) >> space? >> operator.as(:op) >> space? >>
      (literal.as(:value) | str('?')) }
    rule(:expression)  {
      condition >> (space >> str('AND') >> space >> expression).repeat }
    rule(:where)       {
      space >> str('WHERE') >> space >> expression.as_array(:expression) }
  end

  # Identifiers and combinations of them used in queries and updates
  module Identifiers
    include Parslet

    rule(:identifier)    { match('[A-z]').repeat(1).as(:identifier) }
    rule(:select_fields) {
      (identifier | str('**')) >> (comma >> (identifier | str('**'))).repeat }

    rule(:field)         { identifier >> (str('.') >> identifier).repeat(1, 1) }
    rule(:fields)        { field >> (comma >> field).repeat }
    rule(:path)          { identifier >> (str('.') >> identifier).repeat }
  end

  module UpdateSettings
    include Parslet

    rule(:setting) {
      (identifier | str('**')).as(:field) >> space? >> str('=') >> space? >>
      (literal.as(:value) | str('?'))
    }
    rule(:settings) {
      setting >> (space? >> str(',') >> space? >> setting).repeat
    }
  end

  # Parser for a simple CQL-like grammar
  class CQLP < Parslet::Parser
    include Literals
    include Identifiers
    include Predicates
    include UpdateSettings

    rule(:space)       { match('\s').repeat(1) }
    rule(:space?)      { space.maybe }
    rule(:comma)       { str(',') >> space? }

    rule(:limit)       { space >> str('LIMIT') >> space >> integer.as(:limit) }
    rule(:order)       {
      space >> str('ORDER BY') >> space >> fields.as_array(:fields) }

    rule(:query)   {
      str('SELECT') >> space >> (select_fields.as_array(:select) | str('*')) >>
      space >> str('FROM') >> space >> path.as_array(:path) >>
      where.maybe.as(:where) >> order.maybe.as(:order) >>
      limit.maybe.capture(:limit) }

    rule(:update) {
      str('UPDATE') >> space >> path.as_array(:path) >> space >>
      str('SET') >> space >> settings.as_array(:settings) >>
      where.maybe.as(:where).capture_source(:where)
    }

    rule(:insert) {
      str('INSERT INTO') >> space >> identifier.as(:entity) >> space >>
      str('SET') >> space >> settings.as_array(:settings)
    }

    rule(:delete) {
      str('DELETE FROM') >> space >> path.as_array(:path) >>
      where.maybe.as(:where).capture_source(:where)
    }

    rule(:statement) { query | update | insert | delete }

    root :statement
  end

  # Simple transformations to clean up the CQL parse tree
  class CQLT < Parslet::Transform
    rule(identifier: simple(:identifier)) { identifier }
    rule(field: sequence(:id)) { id.map(&:to_s) }
    rule(path: sequence(:id)) { id.map(&:to_s) }
    rule(str: simple(:string)) { string.to_s }
    rule(int: simple(:integer)) { integer.to_i }
    rule(statement: subtree(:stmt)) { stmt.first.last }
  end

  # A single condition in a where clause
  class Condition
    attr_reader :field, :is_range, :operator, :value
    alias_method :range?, :is_range

    def initialize(field, operator, value)
      @field = field
      @operator = operator
      @is_range = [:>, :>=, :<, :<=].include? operator
      @value = value

      freeze
    end

    def inspect
      "#{@field.inspect} #{@operator} #{value}"
    end

    # Compare conditions equal by their field and operator
    def ==(other)
      @field == other.field && @operator == other.operator
    end
  end

  # Used to add a list of conditions to a {Statement}
  module StatementConditions
    attr_reader :conditions

    private

    # Populate the list of condition objects
    def populate_conditions
      @conditions = @tree[:where].nil? ? [] : @tree[:where][:expression]
      @conditions = @conditions.map do |condition|
        field = find_field_with_prefix @tree[:path],
                                       condition[:field]
        value = condition[:value]

        type = field.class.const_get 'TYPE'
        fail TypeError unless type.nil? || value.nil? || value.is_a?(type)

        Condition.new field, condition[:op].to_sym, value
      end

      @eq_fields = @conditions.reject(&:range?).map(&:field).to_set
      @range_field = @conditions.find(&:range?)
      @range_field = @range_field.field unless @range_field.nil?
    end
  end

  # A CQL statement and its associated data
  class Statement
    attr_reader :from, :longest_entity_path, :text, :eq_fields, :range_field

    # Parse either a query or an update
    def self.parse(text, model)
      case text.split.first
      when 'INSERT'
        klass = Insert
      when 'DELETE'
        klass = Delete
      when 'UPDATE'
        klass = Update
      else  # SELECT
        klass = Query
      end

      klass.new text, model
    end

    def initialize(type, text, model)
      @text = text

      # If parsing fails, re-raise as our custom exception
      begin
        @tree = CQLT.new.apply(CQLP.new.method(type).call.parse text)
      rescue Parslet::ParseFailed => exc
        new_exc = ParseFailed.new exc.cause.ascii_tree
        new_exc.set_backtrace exc.backtrace
        raise new_exc
      end

      # XXX Save the where clause so we can convert to a query later
      #     Ideally this would be in {StatementToQuery}
      @where_source = (@tree.delete(:where_source) || '').strip

      @model = model
      path_entities = @tree[:path] || [@tree[:entity]]
      @from = model[path_entities.first.to_s]
      find_longest_path path_entities
    end

    # :nocov:
    def to_color
      "#{@text} [magenta]#{@longest_entity_path.map(&:name).join ', '}[/]"
    end
    # :nocov:

    # Compare statements as equal by their parse tree
    def ==(other)
      other.is_a?(Statement) && @tree == other.instance_variable_get(:@tree)
    end

    private

    # A helper to look up a field based on the path specified in the statement
    def find_field_with_prefix(path, field)
      field_path = field.map(&:to_s)
      prefix_index = path.index(field_path.first)
      field_path = path[0..prefix_index - 1] + field_path \
        unless prefix_index == 0
      @model.find_field field_path.map(&:to_s)
    end

    # Calculate the longest path of entities traversed by the statement
    def find_longest_path(path_entities)
      path = path_entities.map(&:to_s)[1..-1]
      @longest_entity_path = path.reduce [@from] do |entities, key|
        if entities.last.send(:[], key, true)
          # Search through foreign keys
          entities + [entities.last[key].entity]
        else
          # Assume only one foreign key in the opposite direction
          entities + [@model[key]]
        end
      end
    end
  end

  # A representation of a query in the workload
  class Query < Statement
    include StatementConditions

    attr_reader :select, :order, :limit

    def initialize(statement, model)
      super :query, statement, model

      populate_conditions
      populate_fields

      fail InvalidStatementException, 'must have an equality predicate' \
        if @conditions.empty? || @conditions.all?(&:is_range)

      @limit = @tree[:limit].to_i if @tree[:limit]

      if @tree[:where]
        @tree[:where][:expression].each { |condition| condition.delete :value }
      end

      freeze
    end

    # All fields referenced anywhere in the query
    def all_fields
      (@select + @conditions.map(&:field) + @order).to_set
    end

    # This is already a query, no conversion necessary
    def to_query
      self
    end

    private

    # Populate the fields selected by this query
    def populate_fields
      if @tree[:select]
        @select = @tree[:select].map do |field|
          @model.find_field [@from, field.to_s]
        end.to_set
      else
        @select = @from.fields.values.to_set
      end

      return @order = [] if @tree[:order].nil?
      @order = @tree[:order][:fields].map do |field|
        find_field_with_prefix @tree[:path], field
      end
    end
  end

  # The setting of a field from an {Update} statement
  class FieldSetting
    attr_reader :field, :value

    def initialize(field, value)
      @field = field
      @value = value

      freeze
    end

    def inspect
      "#{@field.inspect} = #{value}"
    end

    # Compare settings equal by their field and value
    def ==(other)
      other.field == @field && other.value == @value
    end
  end

  # Module to add variable settings to a {Statement}
  module StatementSettings
    attr_reader :settings

    private

    # Populate all the variable settings
    def populate_settings
      @settings = @tree[:settings].map do |setting|
        field = @from[setting[:field].to_s]
        value = setting[:value]

        type = field.class.const_get 'TYPE'
        fail TypeError unless type.nil? || value.nil? || value.is_a?(type)

        FieldSetting.new field, value
      end
    end
  end

  # Extend {Statement} objects to allow them to be converted to queries
  module StatementToQuery
    # Create a {Query} which corresponds to this update
    def to_query
      # We don't need a query if we're only checking
      # equality on the primary key of a single entity
      needs_query = @longest_entity_path.length > 1
      needs_query ||= @conditions.any?(&:range?)
      needs_query ||= @conditions.map(&:field).to_set != @from.id_fields.to_set
      return unless needs_query

      # Extract the path from the original statement
      query = "SELECT #{@from.id_fields.map(&:name).join ', '} " \
              "FROM #{path_from_statement} #{@where_source}"

      Query.new query.strip, @model
    end

    private

    # Subclasses implement to return the path string from the statement text
    def path_from_statement
      raise NotImplementedError
    end
  end

  # A representation of an update in the workload
  class Update < Statement
    include StatementConditions
    include StatementSettings
    include StatementToQuery

    def initialize(statement, model)
      super :update, statement, model

      populate_conditions
      populate_settings

      freeze
    end

    private

    # Extract the path from the original statement
    def path_from_statement
      /UPDATE\s+(([A-z]+\.)*[A-z]+)\s+/.match(@text).captures.first
    end
  end

  # A representation of an insert in the workload
  class Insert < Statement
    include StatementSettings

    def initialize(statement, model)
      super :insert, statement, model

      populate_settings

      freeze
    end
  end

  # A representation of a delete in the workload
  class Delete < Statement
    include StatementConditions
    include StatementToQuery

    def initialize(statement, model)
      super :delete, statement, model

      populate_conditions

      freeze
    end

    private

    # Extract the path from the original statement
    def path_from_statement
      /DELETE FROM\s+(([A-z]+\.)*[A-z]+)\s+/.match(@text).captures.first
    end
  end

  # Thrown when something tries to parse an invalid statement
  class InvalidStatementException < StandardError
  end

  # Thrown when parsing a statement fails
  class ParseFailed < StandardError
  end
end

# rubocop:enable all
