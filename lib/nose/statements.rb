# frozen_string_literal: true

module NoSE
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
    # @return [Boolean]
    def ==(other)
      @field == other.field && @operator == other.operator
    end
    alias eql? ==

    def hash
      Zlib.crc32 [@field.id, @operator].to_s
    end

    # If the condition is on a foreign key, resolve
    # it to the primary key of the related entity
    # @return [Condition]
    def resolve_foreign_key
      return self unless field.is_a?(Fields::ForeignKeyField)

      Condition.new @field.entity.id_field, @operator, @value
    end
  end

  # Used to add a list of conditions to a {Statement}
  module StatementConditions
    attr_reader :conditions

    # @return [void]
    def populate_conditions(params)
      @conditions = params[:conditions]
      @eq_fields = conditions.each_value.reject(&:range?).map(&:field).to_set
      @range_field = conditions.each_value.find(&:range?)
      @range_field = @range_field.field unless @range_field.nil?
    end

    def self.included(base)
      base.extend ClassMethods
    end

    # Add methods to the class for populating conditions
    module ClassMethods
      private

      # Extract conditions from a parse tree
      # @return [Hash]
      def conditions_from_tree(tree, params)
        conditions = tree[:where].nil? ? [] : tree[:where][:expression]
        conditions = conditions.map { |c| build_condition c, tree, params }

        params[:conditions] = Hash[conditions.map do |condition|
          [condition.field.id, condition]
        end]
      end

      # Construct a condition object from the parse tree
      # @return [void]
      def build_condition(condition, tree, params)
        field = add_field_with_prefix tree[:path], condition[:field], params
        Condition.new field, condition[:op].to_sym,
                      condition_value(condition, field)
      end

      # Get the value of a condition from the parse tree
      # @return [Object]
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

      keys_match = keys.each_cons(2).all? do |prev_key, key|
        key.parent == prev_key.entity
      end
      fail InvalidKeyPathException, 'keys must match along the path' \
        unless keys_match

      @keys = keys
    end

    # Two key paths are equal if their underlying keys are equal or the reverse
    # @return [Boolean]
    def ==(other, check_reverse = true)
      @keys == other.instance_variable_get(:@keys) ||
        (check_reverse && reverse.send(:==, other.reverse, false))
    end
    alias eql? ==

    # Check if this path starts with another path
    # @return [Boolean]
    def start_with?(other, check_reverse = true)
      other_keys = other.instance_variable_get(:@keys)
      @keys[0..other_keys.length - 1] == other_keys ||
        (check_reverse && reverse.start_with?(other.reverse, false))
    end

    # Check if a key is included in the path
    # @return [Boolean]
    def include?(key)
      @keys.include?(key) || entities.any? { |e| e.id_field == key }
    end

    # Combine two key paths by gluing together the keys
    # @return [KeyPath]
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
    # @return [KeyPath]
    def [](index)
      if index.is_a? Range
        keys = @keys[index]
        keys[0] = keys[0].entity.id_field \
          unless keys.empty? || keys[0].instance_of?(Fields::IDField)
        KeyPath.new(keys)
      else
        key = @keys[index]
        key = key.entity.id_field \
          unless key.nil? || key.instance_of?(Fields::IDField)
        key
      end
    end

    # Return the reverse of this path
    # @return [KeyPath]
    def reverse
      KeyPath.new reverse_path
    end

    # Reverse this path in place
    # @return [void]
    def reverse!
      @keys = reverse_path
    end

    # Simple wrapper so that we continue to be a KeyPath
    # @return [KeyPath]
    def to_a
      self
    end

    # Return all the entities along the path
    # @return [Array<Entity>]
    def entities
      @entities ||= @keys.map(&:entity)
    end

    # Split the path where it intersects the given entity
    # @return [KeyPath]
    def split(entity)
      if first.parent == entity
        query_keys = KeyPath.new([entity.id_field])
      else
        query_keys = []
        each do |key|
          query_keys << key
          break if key.is_a?(Fields::ForeignKeyField) && key.entity == entity
        end
        query_keys = KeyPath.new(query_keys)
      end
    end

    # Find where the path intersects the given
    # entity and splice in the target path
    # @return [KeyPath]
    def splice(target, entity)
      split(entity) + target
    end

    # Get the named path to reach this field through the list of keys
    # @return [Array<String>]
    def path_for_field(field)
      return [field.name] if @keys.first.parent == field.parent

      @keys.each_cons(2).take_while do |prev_key, _|
        prev_key.entity != field.parent
      end.map(&:last).map(&:name) << field.name
    end

    # Find the parent of a given field
    # @Return [Entity]
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

    # Produce all subpaths of this path
    # @return [Enumerable<KeyPath>]
    def subpaths(include_self = true)
      Enumerator.new do |enum|
        enum.yield self if include_self
        1.upto(@keys.length) do |i|
          i.upto(@keys.length) do |j|
            enum.yield self[i - 1..j - 1]
          end
        end
      end
    end

    private

    # Get the reverse path
    # @return [Array<Fields::Field>]
    def reverse_path
      return [] if @keys.empty?
      [@keys.last.entity.id_field] + @keys[1..-1].reverse.map(&:reverse)
    end
  end

  # A CQL statement and its associated data
  class Statement
    attr_reader :entity, :key_path, :label, :graph,
                :group, :text, :eq_fields, :range_field, :comment

    # Parse either a query or an update
    def self.parse(text, model, group: nil, label: nil, support: false)
      klass = statement_class text, support
      tree = parse_tree text, klass

      # Ensure we have a valid path in the parse tree
      tree[:path] ||= [tree[:entity]]
      fail InvalidStatementException,
           "FROM clause must start with #{tree[:entity]}" \
           if tree[:entity] && tree[:path].first != tree[:entity]

      params = statement_parameters tree, model
      statement = klass.parse tree, params, text, group: group, label: label
      statement.instance_variable_set :@comment, tree[:comment].to_s

      # Support queries need to populate extra values before finalizing
      unless support
        statement.hash
        statement.freeze
      end

      statement
    end

    # Produce the class of the statement for the given text
    # @return [Class, Symbol]
    def self.statement_class(text, support)
      return SupportQuery if support

      case text.split.first
      when 'INSERT'
        Insert
      when 'DELETE'
        Delete
      when 'UPDATE'
        Update
      when 'CONNECT'
        Connect
      when 'DISCONNECT'
        Disconnect
      else # SELECT
        Query
      end
    end
    private_class_method :statement_class

    # Run the parser and produce the parse tree
    # @raise [ParseFailed]
    # @return [Hash]
    def self.parse_tree(text, klass)
      # Set the type of the statement
      # (but CONNECT and DISCONNECT use the same parse rule)
      type = klass.name.split('::').last.downcase.to_sym
      type = :connect if type == :disconnect

      # If parsing fails, re-raise as our custom exception
      begin
        tree = CQLT.new.apply(CQLP.new.method(type).call.parse(text))
      rescue Parslet::ParseFailed => exc
        new_exc = ParseFailed.new exc.cause.ascii_tree
        new_exc.set_backtrace exc.backtrace
        raise new_exc
      end

      tree
    end
    private_class_method :parse_tree

    # Produce the parameter hash needed to build a new statement
    # @return [Hash]
    def self.statement_parameters(tree, model)
      entity = model[tree[:path].first.to_s]
      key_path = find_longest_path(tree[:path], entity)

      {
        model: model,
        entity: entity,
        key_path: key_path,
        graph: QueryGraph::Graph.from_path(key_path)
      }
    end
    private_class_method :statement_parameters

    # Calculate the longest path of entities traversed by the statement
    # @return [KeyPath]
    def self.find_longest_path(path_entities, from)
      path = path_entities.map(&:to_s)[1..-1]
      longest_entity_path = [from]
      keys = [from.id_field]

      path.each do |key|
        # Search through foreign keys
        last_entity = longest_entity_path.last
        longest_entity_path << last_entity[key].entity
        keys << last_entity[key]
      end

      KeyPath.new(keys)
    end
    private_class_method :find_longest_path

    # A helper to look up a field based on the path specified in the statement
    # @return [Fields::Field]
    def self.add_field_with_prefix(path, field, params)
      field_path = field.map(&:to_s)
      prefix_index = path.index(field_path.first)
      field_path = path[0..prefix_index - 1] + field_path \
        unless prefix_index.zero?
      field_path.map!(&:to_s)

      # Expand the graph to include any keys which were found
      field_path[0..-2].prefixes.drop(1).each do |key_path|
        key = params[:model].find_field key_path
        params[:graph].add_edge key.parent, key.entity, key
      end

      params[:model].find_field field_path
    end
    private_class_method :add_field_with_prefix

    def initialize(params, text, group: nil, label: nil)
      @entity = params[:entity]
      @key_path = params[:key_path]
      @longest_entity_path = @key_path.entities
      @graph = params[:graph]
      @model = params[:model]
      @text = text
      @group = group
      @label = label
    end

    # Specifies if the statement modifies any data
    # @return [Boolean]
    def read_only?
      false
    end

    # Specifies if the statement will require data to be inserted
    # @return [Boolean]
    def requires_insert?(_index)
      false
    end

    # Specifies if the statement will require data to be deleted
    # @return [Boolean]
    def requires_delete?(_index)
      false
    end

    # :nocov:
    def to_color
      "#{@text} [magenta]#{@longest_entity_path.map(&:name).join ', '}[/]"
    end
    # :nocov:

    protected

    # Quote the value of an identifier used as
    # a value for a field, quoted if needed
    # @return [String]
    def maybe_quote(value, field)
      if value.nil?
        '?'
      elsif [Fields::IDField,
             Fields::ForeignKeyField,
             Fields::StringField].include? field.class
        "\"#{value}\""
      else
        value.to_s
      end
    end

    # Generate a string which can be used in the "FROM" clause
    # of a statement or optionally to specify a field
    # @return [String]
    def from_path(path, prefix_path = nil, field = nil)
      if prefix_path.nil?
        from = path.first.parent.name.dup
      else
        # Find where the two paths intersect to get the first path component
        first_key = prefix_path.entries.find do |key|
          path.entities.include?(key.parent) || \
            key.is_a?(Fields::ForeignKeyField) && \
              path.entities.include?(key.entity)
        end
        from = if first_key.primary_key?
                 first_key.parent.name.dup
               else
                 first_key.name.dup
               end
      end

      from << '.' << path.entries[1..-1].map(&:name).join('.') \
        if path.length > 1

      unless field.nil?
        from << '.' unless from.empty?
        from << field.name
      end

      from
    end

    # Produce a string which can be used
    # as the settings clause in a statement
    # @return [String]
    def settings_clause
      'SET ' + @settings.map do |setting|
        value = maybe_quote setting.value, setting.field
        "#{setting.field.name} = #{value}"
      end.join(', ')
    end

    # Produce a string which can be used
    # as the WHERE clause in a statement
    # @return [String]
    def where_clause(field_namer = :to_s.to_proc)
      ' WHERE ' + @conditions.values.map do |condition|
        value = condition.value.nil? ? '?' : condition.value
        "#{field_namer.call condition.field} #{condition.operator} #{value}"
      end.join(' AND ')
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

    # Compare settings equal by their field
    def ==(other)
      other.field == @field
    end
    alias eql? ==

    # Hash by field and value
    def hash
      Zlib.crc32 [@field.id, @value].to_s
    end
  end

  # Module to add variable settings to a {Statement}
  module StatementSettings
    attr_reader :settings

    def self.included(base)
      base.extend ClassMethods
    end

    # Add methods to the class for populating settings
    module ClassMethods
      private

      # Extract settings from a parse tree
      # @return [Array<FieldSetting>]
      def settings_from_tree(tree, params)
        params[:settings] = tree[:settings].map do |setting|
          field = params[:entity][setting[:field].to_s]
          value = setting[:value]

          type = field.class.const_get 'TYPE'
          value = field.class.value_from_string(value.to_s) \
            unless type.nil? || value.nil?

          setting.delete :value
          FieldSetting.new field, value
        end
      end
    end
  end

  # Extend {Statement} objects to allow them to generate support queries
  module StatementSupportQuery
    # Determine if this statement modifies a particular index
    def modifies_index?(index)
      !(@settings.map(&:field).to_set & index.all_fields).empty?
    end

    # Support queries required to updating the given index with this statement
    # @return [Array<SupportQuery>]
    def support_queries(_index)
      []
    end

    private

    # Build a support query to update a given index
    # and select fields with certain conditions
    # @return [SupportQuery]
    def build_support_query(entity, index, graph, select, conditions)
      return nil if select.empty?

      params = {
        select: select,
        graph: graph,
        key_path: graph.longest_path,
        entity: key_path.first.parent,
        conditions: conditions
      }

      support_query = SupportQuery.new entity, params, nil, group: @group
      support_query.instance_variable_set :@statement, self
      support_query.instance_variable_set :@index, index
      support_query.instance_variable_set :@comment, (hash ^ index.hash).to_s
      support_query.instance_variable_set :@text, support_query.unparse
      support_query.hash
      support_query.freeze
    end

    # Produce support queries for the entity of the
    # statement which select the given set of fields
    # @return [Array<SupportQuery>]
    def support_queries_for_entity(index, select)
      graphs = index.graph.size > 1 ? index.graph.split(entity, true) : []

      graphs.map do |graph|
        support_fields = select.select do |field|
          field.parent != entity && graph.entities.include?(field.parent)
        end.to_set

        conditions = {
          entity.id_field.id => Condition.new(entity.id_field, :'=', nil)
        }

        split_entity = split_entity graph, index.graph, entity
        build_support_query split_entity, index, graph, support_fields,
                            conditions
      end.compact
    end

    # Determine which entity a subgraph was split at
    # @return [Entity]
    def split_entity(subgraph, graph, entity)
      graph.keys_from_entity(entity).find do |key|
        subgraph.entities.include? key.entity
      end.entity
    end
  end

  # Thrown when something tries to parse an invalid statement
  class InvalidStatementException < StandardError
  end

  # Thrown when trying to construct a KeyPath which is not valid
  class InvalidKeyPathException < StandardError
  end

  # Thrown when parsing a statement fails
  class ParseFailed < StandardError
  end
end

require_relative 'statements/connection'
require_relative 'statements/delete'
require_relative 'statements/insert'
require_relative 'statements/query'
require_relative 'statements/update'
