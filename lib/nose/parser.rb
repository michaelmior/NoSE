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
  def as_array(name)
    Parslet::Atoms::Named.new(self, name, true)
  end

  # Capture some output along with the source string
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
    rule(:field)         { identifier >> (str('.') >> identifier).repeat(1, 1) }
    rule(:fields)        { field >> (comma >> field).repeat }
    rule(:select_field)  {
      field | (identifier >> str('.') >>
      str('**').as(:identifier2)) | (identifier >> str('.') >>
      str('*').as(:identifier2)) }
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
    rule(identifier: simple(:identifier),
         identifier2: simple(:identifier2)) { [identifier, identifier2] }
    rule(field: sequence(:id)) { id.map(&:to_s) }
    rule(path: sequence(:id)) { id.map(&:to_s) }
    rule(str: simple(:string)) { string.to_s }
    rule(statement: subtree(:stmt)) { stmt.first.last }
    rule(int: simple(:integer)) { integer }
    rule(unknown: simple(:val)) { nil }
  end

  # rubocop:enable all

  # A single condition in a where clause
  class Condition
    attr_reader :field, :is_range, :operator, :value
    alias range? is_range

    def initialize(field, operator, value)
      @field = field
      @operator = operator
      @is_range = [:>, :>=, :<, :<=].include? operator
      @value = value

      # XXX: Not frozen by now to support modification during query execution
      # freeze
    end

    def inspect
      "#{@field.inspect} #{@operator} #{value}"
    end

    # Compare conditions equal by their field and operator
    def ==(other)
      @field == other.field && @operator == other.operator
    end
    alias eql? ==

    def hash
      Zlib.crc32 [@field.id, @operator].to_s
    end
  end

  # Used to add a list of conditions to a {Statement}
  module StatementConditions
    attr_reader :conditions

    private

    # Populate the list of condition objects
    def populate_conditions
      conditions = @tree[:where].nil? ? [] : @tree[:where][:expression]
      conditions = conditions.map { |condition| build_condition condition }

      @eq_fields = conditions.reject(&:range?).map(&:field).to_set
      @range_field = conditions.find(&:range?)
      @range_field = @range_field.field unless @range_field.nil?

      @conditions = Hash[conditions.map do |condition|
        [condition.field.id, condition]
      end]
    end

    # Construct a condition object from the parse tree
    def build_condition(condition)
      field = find_field_with_prefix @tree[:path],
                                     condition[:field]
      Condition.new field, condition[:op].to_sym,
                    condition_value(condition, field)
    end

    # Get the value of a condition from the parse tree
    def condition_value(condition, field)
      value = condition[:value]

      # Convert the value to the correct type
      type = field.class.const_get 'TYPE'
      value = field.class.value_from_string(value.to_s) \
        unless type.nil? || value.nil?

      # Don't allow predicates on foreign keys
      fail InvalidStatementException, 'Predicates cannot use foreign keys' \
        if field.is_a? Fields::ForeignKeyField

      condition.delete :value

      value
    end
  end

  # A path from a primary key to a chain of foreign keys
  class KeyPath
    include Enumerable

    extend Forwardable
    def_delegators :@keys, :each, :inspect, :to_s, :length, :count, :last,
                   :empty?

    def initialize(keys = [])
      fail InvalidKeyPathException, 'first key must be an ID' \
        unless keys.empty? || keys.first.instance_of?(Fields::IDField)

      keys_match = keys.each_cons(2).map do |prev_key, key|
        key.parent == prev_key.entity
      end.all?
      fail InvalidKeyPathException, 'keys must match along the path' \
        unless keys_match

      @keys = keys
    end

    # Two key paths are equal if their underlying keys are equal
    def ==(other)
      @keys == other.instance_variable_get(:@keys)
    end
    alias eql? ==

    # Check if this path starts with another path
    def start_with?(other)
      other_keys = other.instance_variable_get(:@keys)
      @keys[0..other_keys.length - 1] == other_keys
    end

    # Check if a key is included in the path
    def include?(key)
      @keys.include?(key) || entities.any? { |e| e.id_fields.include? key }
    end

    # Combine two key paths by gluing together the keys
    def +(other)
      fail TypeError unless other.is_a? KeyPath
      other_keys = other.instance_variable_get(:@keys)

      # Just copy if there's no combining necessary
      return dup if other_keys.empty?
      return other.dup if @keys.empty?

      # Only allow combining if the entities match
      fail ArgumentError unless other_keys.first.parent == entities.last

      # Combine the two paths
      KeyPath.new(@keys + other_keys[1..-1])
    end

    # Return a slice of the path
    def [](index)
      if index.is_a? Range
        keys = @keys[index]
        keys[0] = keys[0].entity.id_fields.first \
          unless keys.empty? || keys[0].instance_of?(Fields::IDField)
        KeyPath.new(keys)
      else
        key = @keys[index]
        key = key.entity.id_fields.first \
          unless key.nil? || key.instance_of?(Fields::IDField)
        key
      end
    end

    # Return the reverse of this path
    def reverse
      KeyPath.new reverse_path
    end

    # Reverse this path in place
    def reverse!
      @keys = reverse_path
    end

    # Simple wrapper so that we continue to be a KeyPath
    def to_a
      self
    end

    # Return all the entities along the path
    def entities
      @entities ||= @keys.map(&:entity)
    end

    # Find where the path intersects the given
    # entity and splice in the target path
    def splice(target, entity)
      if first.parent == entity
        query_keys = KeyPath.new([entity.id_fields.first])
      else
        query_keys = []
        each do |key|
          query_keys << key
          break if key.is_a?(Fields::ForeignKeyField) && key.entity == entity
        end
        query_keys = KeyPath.new(query_keys)
      end
      query_keys + target
    end

    # Find the parent of a given field
    def find_field_parent(field)
      parent = find do |key|
        field.parent == key.parent ||
        (key.is_a?(Fields::ForeignKeyField) && field.parent == key.entity)
      end

      # This field is not on this portion of the path, so skip
      return nil if parent.nil?

      parent = parent.parent unless parent.is_a?(Fields::ForeignKeyField)
      parent
    end

    private

    # Get the reverse path
    def reverse_path
      return [] if @keys.empty?
      [@keys.last.entity.id_fields.first] + @keys[1..-1].reverse.map(&:reverse)
    end
  end

  # Thrown when trying to construct a KeyPath which is not valid
  class InvalidKeyPathException < StandardError
  end

  # Thrown when parsing a statement fails
  class ParseFailed < StandardError
  end
end
