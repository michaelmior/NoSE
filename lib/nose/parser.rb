# frozen_string_literal: true

require 'parslet'

# rubocop:disable Style/ClassAndModuleChildren

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

# Modify named captures to allow arrays
class Parslet::Atoms::Named < Parslet::Atoms::Base
  def initialize(parslet, name, array = false)
    super()
    @parslet = parslet
    @name = name
    @array = array
  end

  private

  # Optionally wrap the produced single value in an array
  def produce_return_value(val)
    flatval = flatten(val, true)
    flatval = [flatval] if @array && val.last == [:repetition]
    { name => flatval }
  end
end

# Extend the DSL to with some additional ways to capture the output
module Parslet::Atoms::DSL
  # Like #as, but ensures that the result is always an array
  # @return [Array<Parslet::Atoms::Named>]
  def as_array(name)
    Parslet::Atoms::Named.new(self, name, true)
  end

  # Capture some output along with the source string
  # @return [CaptureSource]
  def capture_source(name)
    CaptureSource.new(self, name)
  end
end

# rubocop:enable Style/ClassAndModuleChildren

module NoSE
  # rubocop:disable Style/BlockEndNewline, Style/BlockDelimiters
  # rubocop:disable Style/MultilineOperationIndentation

  # Literals used in queries and updates
  module Literals
    include Parslet

    rule(:integer)     { match('[0-9]').repeat(1).as(:int) }
    rule(:quote)       { str('"') }
    rule(:nonquote)    { quote.absent? >> any }
    rule(:string)      { quote >> nonquote.repeat(1).as(:str) >> quote }
    rule(:literal)     { integer | string | str('?').as(:unknown) }
  end

  # Predicates used in queries and updates
  module Predicates
    include Parslet

    rule(:operator)    {
      str('=') | str('!=') | str('<=') | str('>=') | str('<') | str('>') }
    rule(:condition)   {
      field.as(:field) >> space? >> operator.as(:op) >> space? >>
      literal.as(:value) }
    rule(:expression)  {
      condition >> (space >> str('AND') >> space >> expression).repeat }
    rule(:where)       {
      space >> str('WHERE') >> space >> expression.as_array(:expression) }
  end

  # Identifiers and combinations of them used in queries and updates
  module Identifiers
    include Parslet

    rule(:identifier)    { match('[A-z]').repeat(1).as(:identifier) }
    rule(:field)         { identifier >> (str('.') >> identifier).repeat(1) }
    rule(:fields)        { field >> (comma >> field).repeat }
    rule(:select_field)  {
      field.as_array(:field) | (identifier >> str('.') >>
                                str('*').repeat(1, 2).as(:identifier2)) }
    rule(:select_fields) { select_field >> (comma >> select_field).repeat }
    rule(:path)          { identifier >> (str('.') >> identifier).repeat }
  end

  # Field settings for update and insert statements
  module UpdateSettings
    include Parslet

    rule(:setting) {
      (identifier | str('**')).as(:field) >> space? >> str('=') >> space? >>
      literal.as(:value)
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

    rule(:comment)     { str(' -- ') >> match('.').repeat }

    rule(:query) {
      str('SELECT') >> space >> select_fields.as_array(:select) >>
      space >> str('FROM') >> space >> path.as_array(:path) >>
      where.maybe.as(:where) >> order.maybe.as(:order) >>
      limit.maybe.capture(:limit) >> comment.maybe.as(:comment) }

    rule(:update) {
      str('UPDATE') >> space >> identifier.as(:entity) >> space >>
      (str('FROM') >> space >> path.as_array(:path) >> space).maybe >>
      str('SET') >> space >> settings.as_array(:settings) >>
      where.maybe.as(:where).capture_source(:where) >>
      comment.maybe.as(:comment)
    }

    rule(:connect_item) {
      identifier.as(:target) >> space? >> str('(') >> space? >>
      literal.as(:target_pk) >> space? >> str(')')
    }

    rule(:connect_list) {
      connect_item >> (space? >> str(',') >> space? >> connect_item).repeat
    }

    rule(:insert) {
      str('INSERT INTO') >> space >> identifier.as(:entity) >> space >>
      str('SET') >> space >> settings.as_array(:settings) >>
      (space >> str('AND') >> space >> str('CONNECT') >> space >>
       str('TO') >> space >> connect_list.as_array(:connections)).maybe >>
      comment.maybe.as(:comment)
    }

    rule(:delete) {
      str('DELETE') >> space >> identifier.as(:entity) >>
      (space >> str('FROM') >> space >> path.as_array(:path)).maybe >>
      where.maybe.as(:where).capture_source(:where) >>
      comment.maybe.as(:comment)
    }

    rule(:connect) {
      (str('CONNECT') | str('DISCONNECT')).capture(:type) >> space >>
      identifier.as(:entity) >> space? >> str('(') >> space? >>
      literal.as(:source_pk) >> space? >> str(')') >> space >>
      dynamic do |_, context|
        context.captures[:type] == 'CONNECT' ? str('TO') : str('FROM')
      end >> space >> connect_item
    }

    rule(:statement) {
      query | update | insert | delete | connect
    }

    root :statement
  end

  # Simple transformations to clean up the CQL parse tree
  class CQLT < Parslet::Transform
    rule(identifier: simple(:identifier)) { identifier }
    rule(identifier: simple(:identifier), identifier2: simple(:identifier2)) {
      [identifier.to_s, identifier2.to_s] }
    rule(field: sequence(:id)) { id.map(&:to_s) }
    rule(path: sequence(:id)) { id.map(&:to_s) }
    rule(str: simple(:string)) { string.to_s }
    rule(statement: subtree(:stmt)) { stmt.first.last }
    rule(int: simple(:integer)) { integer }
    rule(unknown: simple(:val)) { nil }
  end

  # rubocop:enable all
end
