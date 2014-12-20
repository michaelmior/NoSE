require 'parslet'

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

    rule(:field)       { identifier >> (str('.') >> identifier).repeat(1) }
    rule(:fields)      { field >> (comma >> field).repeat }

    rule(:condition)   {
      field.as(:field) >> space >> operator.as(:op) >> space? >> \
      (literal.as(:value) | str('?')) }
    rule(:expression)  {
      condition >> (space >> str('AND') >> space >> expression).repeat }
    rule(:where)       {
      space >> str('WHERE') >> space >> expression.as_array(:expression) }

    rule(:limit)       { space >> str('LIMIT') >> space >> integer.as(:limit) }
    rule(:order)       {
      space >> str('ORDER BY') >> space >> fields.as_array(:fields) }

    rule(:statement)   {
      str('SELECT') >> space >> (identifiers.as_array(:select) | str('*')) >> \
      space >> str('FROM') >> space >> identifier.as(:entity) >> \
      where.maybe.as(:where) >> order.maybe.as(:order) >> \
      limit.maybe.capture(:limit) }
    root :statement
  end

  # Simple transformations to clean up the CQL parse tree
  class CQLT < Parslet::Transform
    rule(identifier: simple(:identifier)) { identifier }
    rule(field: sequence(:field)) { field.map(&:to_s) }
    rule(str: simple(:string)) { string.to_s }
    rule(int: simple(:integer)) { integer.to_i }
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
  end

  # A CQL statement and its associated data
  class Statement
    attr_reader :select, :from, :conditions, :order, :limit,
                :eq_fields, :range_field,
                :longest_entity_path, :query

    def initialize(query, workload)
      @query = query

      # If parsing fails, re-raise as our custom exception
      begin
        tree = CQLT.new.apply(CQLP.new.parse query)
      rescue Parslet::ParseFailed => exc
        new_exc = ParseFailed.new exc.message
        new_exc.set_backtrace exc.backtrace
        raise new_exc
      end

      @from = workload[tree[:entity].to_s]

      populate_fields tree, workload
      populate_conditions tree[:where], workload

      fail InvalidQueryException, 'must have at least one equality predicate' \
        if @conditions.empty? || @conditions.all?(&:is_range)

      @limit = tree[:limit].to_i if tree[:limit]

      find_longest_path tree, workload

      @tree = tree
      if tree[:where]
        tree[:where][:expression].each { |condition| condition.delete :value }
      end

      freeze
    end

    # :nocov:
    def to_color
      "#{@query} [magenta]#{@longest_entity_path.map(&:name).join ', '}[/]"
    end
    # :nocov:

    # All fields referenced anywhere in the query
    def all_fields
      (@select + @conditions.map(&:field) + @order).to_set
    end

    # Compare statements as equal by their parse tree
    def ==(other)
      other.is_a?(Statement) && @tree == other.instance_variable_get(:@tree)
    end

    private

    # Populate the fields selected by this query
    def populate_fields(tree, workload)
      if tree[:select]
        @select = tree[:select].map do |field|
          workload.find_field [tree[:entity].to_s, field.to_s]
        end.to_set
      else
        @select = @from.fields.values.to_set
      end

      return @order = [] if tree[:order].nil?
      @order = tree[:order][:fields].map do |field|
        workload.find_field field.map(&:to_s)
      end
    end

    # Populate the list of condition objects
    def populate_conditions(where, workload)
      if where.nil?
        @conditions = []
      else
        @conditions = where[:expression].map do |condition|
          field = workload.find_field condition[:field].map(&:to_s)
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

    # Calculate the longest path of entities traversed by the query
    def find_longest_path(tree, workload)
      return @longest_entity_path = [@from] if tree[:where].nil?
      where = tree[:where][:expression]
      return @longest_entity_path = [@from] if where.length == 0

      fields = where.map { |condition| condition[:field].map(&:to_s) }
      fields += tree[:order][:fields].map { |field| field.map(&:to_s) } \
        unless tree[:order].nil?
      path = fields.max_by(&:length)[1..-2]  # end is field
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
  end

  # Thrown when something tries to parse an invalid query
  class InvalidQueryException < StandardError
  end

  class ParseFailed < StandardError
  end
end

# rubocop:enable all
