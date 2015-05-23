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

# Modify named captures to allow arrays
class Parslet::Atoms::Named < Parslet::Atoms::Base
  def initialize(parslet, name, array = false)
    super()
    @parslet, @name, @array = parslet, name, array
  end

  private

  def produce_return_value(val)
    flatval = flatten(val, true)
    flatval = [flatval] if @array and val.last == [:repetition]
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

# rubocop:disable Style/Blocks, Style/BlockEndNewline

module NoSE
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
    rule(:select_field)  { field | (identifier >> str('.') >>
                                    str('**').as(:identifier2)) |
                           (identifier >> str('.') >>
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

    rule(:query)   {
      str('SELECT') >> space >> select_fields.as_array(:select) >>
      space >> str('FROM') >> space >> path.as_array(:path) >>
      where.maybe.as(:where) >> order.maybe.as(:order) >>
      limit.maybe.capture(:limit) }

    rule(:update) {
      str('UPDATE') >> space >> identifier.as(:entity) >> space >>
      (str('FROM') >> space >> path.as_array(:path) >> space).maybe >>
      str('SET') >> space >> settings.as_array(:settings) >>
      where.maybe.as(:where).capture_source(:where)
    }

    rule(:insert) {
      str('INSERT INTO') >> space >> identifier.as(:entity) >> space >>
      str('SET') >> space >> settings.as_array(:settings)
    }

    rule(:delete) {
      str('DELETE') >> space >> identifier.as(:entity) >>
      (space >> str('FROM') >> space >> path.as_array(:path)).maybe >>
      where.maybe.as(:where).capture_source(:where)
    }

    rule(:connect) {
      (str('CONNECT') | str('DISCONNECT')).capture(:type) >> space >>
      identifier.as(:entity) >> space? >> str('(') >> space? >>
      literal.as(:source_pk) >> space? >> str(')') >> space >>
      dynamic do |_, context|
        context.captures[:type] == 'CONNECT' ? str('TO') : str('FROM')
      end >>
      space >> identifier.as(:target) >> space? >> str('(') >> space? >>
      literal.as_array(:target_pk) >> space? >> str(')')
    }

    rule(:statement) {
      query | update | insert | delete | connect
    }

    root :statement
  end

  # Simple transformations to clean up the CQL parse tree
  class CQLT < Parslet::Transform
    rule(identifier: simple(:identifier)) { identifier }
    rule(identifier: simple(:identifier), identifier2: simple(:identifier2)) { [identifier, identifier2] }
    rule(field: sequence(:id)) { id.map(&:to_s) }
    rule(path: sequence(:id)) { id.map(&:to_s) }
    rule(str: simple(:string)) { string.to_s }
    rule(int: simple(:integer)) { integer.to_i }
    rule(statement: subtree(:stmt)) { stmt.first.last }
    rule(unknown: simple(:val)) { nil }
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
    alias_method :eql?, :==

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
      @conditions = @tree[:where].nil? ? [] : @tree[:where][:expression]
      @conditions = @conditions.map do |condition|
        field = find_field_with_prefix @tree[:path],
                                       condition[:field]
        value = condition[:value]

        type = field.class.const_get 'TYPE'
        fail TypeError unless type.nil? || value.nil? || value.is_a?(type)

        # Don't allow predicates on foreign keys
        fail InvalidStatementException, 'Predicates cannot use foreign keys' \
          if field.is_a? Fields::ForeignKeyField

        Condition.new field, condition[:op].to_sym, value
      end

      @eq_fields = @conditions.reject(&:range?).map(&:field).to_set
      @range_field = @conditions.find(&:range?)
      @range_field = @range_field.field unless @range_field.nil?
    end
  end

  class KeyPath
    include Enumerable

    extend Forwardable
    def_delegators :@keys, :each,  :inspect, :to_s, :length, :count, :last

    def initialize(keys = [])
      @keys = keys
    end

    # Two key paths are equal if their underlying keys are equal
    def ==(other)
      @keys == other.instance_variable_get(:@keys)
    end
    alias_method :eql?, :==

    # Combine two key paths by gluing together the keys
    def +(other)
      fail TypeError unless other.is_a? KeyPath
      other_keys = other.instance_variable_get(:@keys)

      # Just copy if there's no combining necessary
      return self.dup if other_keys.empty?
      return other.dup if @keys.empty?

      # Only allow combining if the entities match
      fail ArgumentError unless other_keys.first.parent == entities.to_a.last

      # Combine the two paths
      KeyPath.new(@keys + other_keys[1..-1])
    end

    # Return a slice of the path
    def [](index)
      if index.is_a? Range
        keys = @keys[index]
        keys[0] = keys[0].entity.id_fields.first \
          unless keys.empty? || keys[0].instance_of?(NoSE::Fields::IDField)
        KeyPath.new(keys)
      else
        key = @keys[index]
        key.entity.id_fields.first \
          unless key.nil? || key.instance_of?(NoSE::Fields::IDField)
        key
      end
    end

    # Return the reverse of this path
    def reverse
      path = @keys.reverse
      if path.length > 1
        path[0] = path[0].entity.id_fields.first  # XXX broken for composite
        path[-1] = @keys[1].reverse
      end

      KeyPath.new(path)
    end

    # Simple wrapper so that we continue to be a KeyPath
    def to_a
      self
    end

    # Return all the entities along the path
    def entities
      Enumerator.new do |enum|
        enum.yield @keys.first.parent
        @keys[1..-1].each { |key| enum.yield key.entity }
      end
    end
  end

  # A CQL statement and its associated data
  class Statement
    attr_reader :from, :longest_entity_path, :key_path,
                :text, :eq_fields, :range_field

    # Parse either a query or an update
    def self.parse(text, model)
      case text.split.first
      when 'INSERT'
        klass = Insert
      when 'DELETE'
        klass = Delete
      when 'UPDATE'
        klass = Update
      when 'CONNECT'
        klass = Connect
      when 'DISCONNECT'
        klass = Disconnect
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
      #     Ideally this would be in {StatementSupportQuery}
      @where_source = (@tree.delete(:where_source) || '').strip

      @model = model
      @tree[:path] ||= [@tree[:entity]]
      fail InvalidStatementException,
           "FROM clause must start with #{@tree[:entity]}" \
           if @tree[:entity] && @tree[:path].first != @tree[:entity]

      @from = model[@tree[:path].first.to_s]
      find_longest_path @tree[:path]
    end

    # Specifies if the statement modifies any data
    def read_only?
      false
    end

    # Specifies if the statement will require data to be inserted
    def requires_insert?
      false
    end

    # Specifies if the statement will require data to be deleted
    def requires_delete?
      false
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
    alias_method :eql?, :==

    def hash
      Zlib.crc32 @tree.to_s
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
      @longest_entity_path = [@from]
      keys = [@from.id_fields.first]  # XXX broken for composite keys

      path.each do |key|
        # Search through foreign keys
        last_entity = @longest_entity_path.last
        @longest_entity_path << last_entity[key].entity
        keys << last_entity[key]
      end

      @key_path = KeyPath.new(keys)
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

    # Specifies that queries don't modify data
    def read_only?
      true
    end

    # All fields referenced anywhere in the query
    def all_fields
      (@select + @conditions.map(&:field) + @order).to_set
    end

    private

    # Populate the fields selected by this query
    def populate_fields
      @select = @tree[:select].flatten.each_slice(2).map do |field|
        # Find the entity along the path
        entity = longest_entity_path[@tree[:path].index(field.first)]

        if field.last == '*'
          entity.fields.values
        else
          field = @model.find_field [entity, field.last.to_s]

          fail InvalidStatementException, 'Foreign keys cannot be selected' \
            if field.is_a? Fields::ForeignKeyField

          field
        end
      end.flatten(1).to_set

      return @order = [] if @tree[:order].nil?
      @order = @tree[:order][:fields].map do |field|
        find_field_with_prefix @tree[:path], field
      end
    end
  end

  # A query required to support an update
  class SupportQuery < Query
    attr_reader :statement, :index

    def initialize(query, model, statement, index)
      @statement = statement
      @index = index

      super query, model
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
    alias_method :eql?, :==

    # Hash by field and value
    def hash
      Zlib.crc32 [@field.id, @value].to_s
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

  # Extend {Statement} objects to allow them to generate support queries
  module StatementSupportQuery
    # Determine if this statement modifies a particular index
    def modifies_index?(index)
      !(@settings.map(&:field).to_set & index.all_fields).empty?
    end

    protected

    # Get the support query for updating a given
    # set of fields for a particular index
    def support_query_for_fields(index, fields)
      return nil if fields.empty?

      # Get the new KeyPath for the support query
      query_keys = splice_path index.path, @key_path, @from
      query_from = query_keys.map(&:name)
      query_from[0] = query_keys.first.parent.name

      # Don't require selecting fields given in the WHERE clause or settings
      given_fields = self.is_a?(Insert) ? @settings : @conditions
      required_fields = index.hash_fields - given_fields.map(&:field)
      return nil if required_fields.empty?

      # Get the full name of each field to be used during selection
      required_fields.map! do |field|
        parent = query_keys.find do |key|
          field.parent == key.parent ||
          (key.is_a?(NoSE::Fields::ForeignKeyField) &&
           field.parent == key.entity)
        end
        parent = parent.parent \
          unless parent.is_a?(NoSE::Fields::ForeignKeyField)

        "#{parent.name}.#{field.name}"
      end

      query = "SELECT #{required_fields.to_a.join ', ' } " \
              "FROM #{query_from.join '.'}"
      query += " #{@where_source}" unless @where_source.empty?
      SupportQuery.new query, @model, self, index
    end

    private

    # Find where the index path intersects the update path
    # and splice in the path of the where clause from the update
    def splice_path(source, target, from)
      if source.first.parent == from
        query_keys = KeyPath.new([from.id_fields.first])
      else
        query_keys = KeyPath.new(source.each_cons(2).take_while do |key, _|
          next true if key.instance_of?(NoSE::Fields::IDField)
          key.entity == from
        end.flatten(1))
      end
      query_keys + target
    end
  end

  # A representation of an update in the workload
  class Update < Statement
    include StatementConditions
    include StatementSettings
    include StatementSupportQuery

    def initialize(statement, model)
      super :update, statement, model

      populate_conditions
      populate_settings

      freeze
    end

    # Specifies that updates require insertion
    def requires_insert?
      true
    end

    # Specifies that updates require deletion
    def requires_delete?
      true
    end

    # Get the support queries for updating an index
    def support_queries(index)
      # Get the updated fields and check if an update is necessary
      updated_fields = settings.map(&:field).to_set & index.all_fields
      [support_query_for_fields(index, updated_fields)].compact
    end
  end

  # A representation of an insert in the workload
  class Insert < Statement
    include StatementSettings
    include StatementSupportQuery

    alias_method :entity, :from

    def initialize(statement, model)
      super :insert, statement, model

      populate_settings

      freeze
    end

    # Specifies that inserts require insertion
    def requires_insert?
      true
    end

    # Get the support queries for inserting into an index
    def support_queries(index)
      # XXX We should be able to do this with at most two queries,
      #     one for each side of the branch down the query path
      return [] if (@from.fields.values.to_set & index.all_fields).empty?
      index.all_fields.group_by(&:parent).map do |_, fields|
        support_query_for_fields index, fields
      end.compact
    end
  end

  # A representation of a delete in the workload
  class Delete < Statement
    include StatementConditions
    include StatementSupportQuery

    def initialize(statement, model)
      super :delete, statement, model

      populate_conditions

      freeze
    end

    # Specifies that deletes require deletion
    def requires_delete?
      true
    end

    # Get the support queries for deleting from an index
    def support_queries(index)
      [support_query_for_fields(index, @from.fields)].compact
    end
  end

  # Superclass for connect and disconnect statements
  class Connection < Statement
    attr_reader :source_pk, :target, :target_pk
    alias_method :source, :from

    protected

    # Populate the keys and entities
    def populate_keys
      @source_pk = @tree[:source_pk]
      @target = @from.foreign_keys[@tree[:target].to_s]
      @target_pk = @tree[:target_pk]

      # XXX Only works for non-composite PKs
      source_type = @from.id_fields.first.class.const_get 'TYPE'
      fail TypeError unless source_type.nil? || source_pk.nil? ||
                            source_pk.is_a?(type)

      target_type = @target.class.const_get 'TYPE'
      fail TypeError unless target_type.nil? || target_pk.nil? ||
                            target_pk.is_a?(type)
    end
  end

  # A representation of a connect in the workload
  class Connect < Connection
    def initialize(statement, model)
      super :connect, statement, model
      fail InvalidStatementException, 'DISCONNECT parsed as CONNECT' \
        unless @text.split.first == 'CONNECT'
      populate_keys
      freeze
    end
  end

  # A representation of a disconnect in the workload
  class Disconnect < Connection
    def initialize(statement, model)
      super :connect, statement, model
      fail InvalidStatementException, 'CONNECT parsed as DISCONNECT' \
        unless @text.split.first == 'DISCONNECT'
      populate_keys
      freeze
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
