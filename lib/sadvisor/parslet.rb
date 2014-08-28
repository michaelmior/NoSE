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
end

# rubocop:enable all
