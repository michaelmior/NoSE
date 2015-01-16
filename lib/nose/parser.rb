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
  # Parser for a simple CQL-like grammar
  class CQLP < Parslet::Parser
    rule(:operator)    {
      str('=') | str('!=') | str('<=') | str('>=') | str('<') | str('>') }
    rule(:space)       { match('\s').repeat(1) }
    rule(:space?)      { space.maybe }
    rule(:comma)       { str(',') >> space? }
    rule(:integer)     { match('[0-9]').repeat(1).as(:int) }
    rule(:quote)       { str('"') }
    rule(:nonquote)    { quote.absent? >> any }
    rule(:string)      { quote >> nonquote.repeat(1).as(:str) >> quote }
    rule(:literal)     { integer | string }

    rule(:identifier)  { match('[A-z]').repeat(1).as(:identifier) }
    rule(:identifiers) { identifier >> (comma >> identifier).repeat }

    rule(:field)       { identifier >> (str('.') >> identifier).repeat(1, 1) }
    rule(:fields)      { field >> (comma >> field).repeat }
    rule(:path)        { identifier >> (str('.') >> identifier).repeat }

    rule(:condition)   {
      field.as(:field) >> space? >> operator.as(:op) >> space? >>
      (literal.as(:value) | str('?')) }
    rule(:expression)  {
      condition >> (space >> str('AND') >> space >> expression).repeat }
    rule(:where)       {
      space >> str('WHERE') >> space >> expression.as_array(:expression) }

    rule(:limit)       { space >> str('LIMIT') >> space >> integer.as(:limit) }
    rule(:order)       {
      space >> str('ORDER BY') >> space >> fields.as_array(:fields) }

    rule(:query)   {
      str('SELECT') >> space >> (identifiers.as_array(:select) | str('*')) >>
      space >> str('FROM') >> space >> path.as_array(:path) >>
      where.maybe.as(:where) >> order.maybe.as(:order) >>
      limit.maybe.capture(:limit) }

    rule(:setting) {
      identifier.as(:field) >> space? >> str('=') >> space? >>
      (literal.as(:value) | str('?'))
    }
    rule(:settings) {
      setting >> (space? >> str(',') >> space? >> setting).repeat
    }
    rule(:update) {
      str('UPDATE') >> space >> path.as_array(:path) >> space >>
      str('SET') >> space >> settings.as_array(:settings) >>
      where.maybe.as(:where).capture_source(:where)
    }

    rule(:statement) { query | update }

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

  # A CQL statement and its associated data
  class Statement
    attr_reader :conditions, :from, :longest_entity_path, :query,
                :eq_fields, :range_field

    # Parse either a query or an update
    def self.parse(query, workload)
      klass = query.start_with?('SELECT ') ? Query : Update
      klass.new query, workload
    end

    def initialize(type, query, workload)
      @query = query

      # If parsing fails, re-raise as our custom exception
      begin
        @tree = CQLT.new.apply(CQLP.new.method(type).call.parse query)
      rescue Parslet::ParseFailed => exc
        new_exc = ParseFailed.new exc.cause.ascii_tree
        new_exc.set_backtrace exc.backtrace
        raise new_exc
      end

      @from = workload[@tree[:path].first.to_s]
      find_longest_path workload

      populate_conditions workload
    end

    # :nocov:
    def to_color
      "#{@query} [magenta]#{@longest_entity_path.map(&:name).join ', '}[/]"
    end
    # :nocov:

    # Compare statements as equal by their parse tree
    def ==(other)
      other.is_a?(Statement) && @tree == other.instance_variable_get(:@tree)
    end

    private

    # A helper to look up a field based on the path specified in the statement
    def find_field_with_prefix(workload, path, field)
      field_path = field.map(&:to_s)
      prefix_index = path.index(field_path.first)
      field_path = path[0..prefix_index - 1] + field_path \
        unless prefix_index == 0
      workload.find_field field_path.map(&:to_s)
    end

    # Calculate the longest path of entities traversed by the query
    def find_longest_path(workload)
      path = @tree[:path].map(&:to_s)[1..-1]
      @longest_entity_path = path.reduce [@from] do |entities, key|
        if entities.last.send(:[], key, true)
          # Search through foreign keys
          entities + [entities.last[key].entity]
        else
          # Assume only one foreign key in the opposite direction
          entities + [workload[key]]
        end
      end
    end

    # Populate the list of condition objects
    def populate_conditions(workload)
      if @tree[:where].nil?
        @conditions = []
      else
        @conditions = @tree[:where][:expression].map do |condition|
          field = find_field_with_prefix workload, @tree[:path],
            condition[:field]
          value = condition[:value]

          type = field.class.const_get 'TYPE'
          fail TypeError unless type.nil? || value.nil? || value.is_a?(type)

          Condition.new field, condition[:op].to_sym, value
        end
      end

      @eq_fields = @conditions.reject(&:range?).map(&:field).to_set
      @range_field = @conditions.find(&:range?)
      @range_field = @range_field.field unless @range_field.nil?
    end
  end

  # A representation of a query in the workload
  class Query < Statement
    attr_reader :select, :order, :limit

    def initialize(query, workload)
      super :query, query, workload

      populate_fields workload

      fail InvalidQueryException, 'must have at least one equality predicate' \
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

    private

    # Populate the fields selected by this query
    def populate_fields(workload)
      if @tree[:select]
        @select = @tree[:select].map do |field|
          workload.find_field [@from, field.to_s]
        end.to_set
      else
        @select = @from.fields.values.to_set
      end

      return @order = [] if @tree[:order].nil?
      @order = @tree[:order][:fields].map do |field|
        find_field_with_prefix workload, @tree[:path], field
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

  # A representation of an update the workload
  class Update < Statement
    attr_accessor :settings

    def initialize(query, workload)
      super :update, query, workload

      populate_settings workload

      # Save the where clause so we can convert to a query later
      @workload = workload
      @where_source = @tree.delete(:where_source).strip

      freeze
    end

    # Populate all the variable settings
    def populate_settings(workload)
      @settings = @tree[:settings].map do |setting|
        field = workload[@from][setting[:field].to_s]
        value = setting[:value]

        type = field.class.const_get 'TYPE'
        fail TypeError unless type.nil? || value.nil? || value.is_a?(type)

        FieldSetting.new field, value
      end
    end

    # Create a {Query} which corresponds to this update
    def to_query
      # Extract the path from the original query
      path = /UPDATE\s+(([A-z]+\.)*[A-z]+)\s+/.match(@query).captures.first
      query = "SELECT #{@settings.map(&:field).map(&:name).join ', '} " \
              "FROM #{path} #{@where_source}"

      Query.new query.strip, @workload
    end
  end

  # Thrown when something tries to parse an invalid query
  class InvalidQueryException < StandardError
  end

  # Thrown when parsing a statement fails
  class ParseFailed < StandardError
  end
end

# rubocop:enable all
