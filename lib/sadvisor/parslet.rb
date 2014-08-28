require 'parslet'

# rubocop:disable Style/Blocks, Style/BlockEndNewline

module Sadvisor
  # Parser for a simple CQL-like grammar
  class CQLP < Parslet::Parser
    rule(:operator)    {
      str('=') | str('!=') | str('<=') | str('>=') | str('<') | str('>') }
    rule(:space)       { match('\s').repeat(1) }
    rule(:space?)      { space.maybe }
    rule(:comma)       { str(',') >> space? }
    rule(:literal)     { str('?') }
    rule(:integer)     { match('[0-9]').repeat(1) }

    rule(:identifier)  { match('[A-z]').repeat(1).as(:identifier) }
    rule(:identifiers) { identifier >> (comma >> identifier).repeat }

    rule(:field)       {
      (identifier >> (str('.') >> identifier).repeat(1)).as_array(:field) }
    rule(:fields)      { field >> (comma >> field).repeat }

    rule(:condition)   {
      field.as(:field) >> space >> operator.as(:op) >> space? >> literal }
    rule(:expression)  {
      condition >> (space >> str('AND') >> space >> expression).repeat }
    rule(:where)       {
      space >> str('WHERE') >> space >> expression.as_array(:expression) }

    rule(:limit)       { space >> str('LIMIT') >> space >> integer.as(:limit) }
    rule(:order)       {
      space >> str('ORDER BY') >> space >> fields.as_array(:fields) }

    rule(:statement)   {
      str('SELECT') >> space >> identifiers.as_array(:select) >> space >> \
      str('FROM') >> space >> identifier.as(:entity) >> \
      where.maybe.as_array(:where) >> order.maybe.as(:order) >> \
      limit.maybe.capture(:limit) }
    root :statement
  end

  # Simple transformations to clean up the CQL parse tree
  class CQLT < Parslet::Transform
    rule(identifier: simple(:identifier)) { identifier }
    rule(field: sequence(:field)) { field.map(&:to_s) }
  end

  # A single condition in a where clause
  class Condition
    attr_reader :field, :is_range
    alias_method :range?, :is_range

    def initialize(field, operator)
      @field = field
      @is_range = [:>, :>=, :<, :<=].include? operator
    end

    def inspect
      @field.inspect + ' ' + @is_range.inspect
    end
  end

  # A CQL statement and its associated data
  class Statement
    attr_reader :select, :from, :conditions, :order, :limit

    def initialize(query, workload)
      tree = CQLT.new.apply(CQLP.new.parse query)
      @from = workload[tree[:entity].to_s]
      @select = tree[:select].map do |field|
        workload.find_field [tree[:entity].to_s, field.to_s]
      end

      populate_conditions tree[:where][:expression], workload

      @order = tree[:order][:fields].map do |field|
        workload.find_field field.map(&:to_s)
      end
      @limit = tree[:limit].to_i if tree[:limit]
    end

    private

    def populate_conditions(where, workload)
      @conditions = where.map do |condition|
        field = workload.find_field condition[:field].map(&:to_s)
        Condition.new field, condition[:op].to_sym
      end
    end
  end
end

# rubocop:enable all
