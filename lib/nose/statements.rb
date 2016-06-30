module NoSE
  # A CQL statement and its associated data
  class Statement
    attr_reader :from, :longest_entity_path, :key_path, :label, :graph,
                :group, :text, :eq_fields, :range_field

    # Parse either a query or an update
    def self.parse(text, model, group: nil, label: label)
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
      else # SELECT
        klass = Query
      end

      klass.new text, model, group: group, label: label
    end

    def initialize(type, text, model, group: nil, label: label)
      @group = group
      @label = label
      @text = text

      # If parsing fails, re-raise as our custom exception
      begin
        @tree = CQLT.new.apply(CQLP.new.method(type).call.parse text)
      rescue Parslet::ParseFailed => exc
        new_exc = ParseFailed.new exc.cause.ascii_tree
        new_exc.set_backtrace exc.backtrace
        raise new_exc
      end

      # TODO: Ignore comments, this is needed as a hack so otherwise identical
      #       queries can be treated differently everywhere
      # @tree.delete(:comment)

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
      @graph = QueryGraph::Graph.from_path(@key_path.reverse)
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

    # Compare statements as equal by their parse tree
    def ==(other)
      other.is_a?(Statement) && @tree == other.instance_variable_get(:@tree)
    end
    alias eql? ==

    def hash
      Zlib.crc32 @tree.to_s
    end

    private

    # A helper to look up a field based on the path specified in the statement
    # @return [Fields::Field]
    def find_field_with_prefix(path, field)
      field_path = field.map(&:to_s)
      prefix_index = path.index(field_path.first)
      field_path = path[0..prefix_index - 1] + field_path \
        unless prefix_index == 0
      @model.find_field field_path.map(&:to_s)
    end

    # Calculate the longest path of entities traversed by the statement
    # @return [KeyPath]
    def find_longest_path(path_entities)
      path = path_entities.map(&:to_s)[1..-1]
      @longest_entity_path = [@from]
      keys = [@from.id_fields.first] # XXX broken for composite keys

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

    def initialize(statement, model, group: nil, label: label)
      super :query, statement, model, group: group, label: label

      populate_conditions
      populate_fields

      fail InvalidStatementException, 'must have an equality predicate' \
        if @conditions.empty? || @conditions.values.all?(&:is_range)

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
    # @return [Set<Fields::Field>]
    def all_fields
      (@select + @conditions.each_value.map(&:field) + @order).to_set
    end

    private

    # Populate the fields selected by this query
    # @return [void]
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
      @order = @tree[:order][:fields].each_slice(2).map do |field|
        field = field.first if field.first.is_a?(Array)
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

    # Support queries must also have their statement and index checked
    def ==(other)
      other.is_a?(SupportQuery) && @statement == other.statement &&
        @index == other.index
    end
    alias eql? ==

    def hash
      Zlib.crc32_combine super, @index.hash, @index.hash_str.length
    end

    # :nocov:
    def to_color
      super.to_color + ' for [magenta]' + @index.key + '[/]'
    end
    # :nocov:
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
    alias eql? ==

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
    # @return [void]
    def populate_settings
      @settings = @tree[:settings].map do |setting|
        field = @from[setting[:field].to_s]
        value = setting[:value]

        type = field.class.const_get 'TYPE'
        value = field.class.value_from_string(value.to_s) \
          unless type.nil? || value.nil?

        setting.delete :value
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

    # Support queries required to updating the given index with this statement
    # @return [Array<SupportQuery>]
    def support_queries(_index)
      []
    end

    # The fields available with this statement
    # @return [Array<Fields::Field>]
    def given_fields
      []
    end

    protected

    # Produce the support query for an index on the given path
    # @return [SupportQuery]
    def support_query_for_path(index, query_keys, where = nil, all = false)
      # If this portion of the path is empty, then we have no support query
      return nil if query_keys.empty?

      query_from = query_keys.map(&:name)
      query_from[0] = query_keys.first.parent.name

      # Check if we actually need any fields
      required_fields = required_fields index, query_keys, all
      return nil if required_fields.empty?

      # Reconstruct a valid where clause with the new path
      where = 'WHERE ' + @conditions.each_value.map do |condition|
        parent = query_keys.find_field_parent condition.field
        value = condition.value.nil? ? '?' : condition.value

        "#{parent.name}.#{condition.field.name} #{condition.operator} #{value}"
      end.join(' AND ') if where.nil?

      query = "SELECT #{required_fields.to_a.join(', ')} " \
              "FROM #{query_from.join '.'} #{where}"

      # XXX This should not be necessary, but we need it
      #     for now to keep each individual query unique
      query += " -- #{query.hash}"

      SupportQuery.new query, @model, self, index
    end

    # Get the support query for updating a given
    # set of fields for a particular index
    # @return [SupportQuery]
    def support_query_for_fields(index, fields, all = false)
      # If this index is not modified, definitely no support queries needed
      return nil unless modifies_index?(index)

      # Simple check to see if no fields are updated
      return nil if fields.empty?

      # Get the new KeyPath for the support query based on the longest
      # path from the intersection with the statement and index paths
      path1 = index.path.splice @key_path, @from
      path2 = index.path.reverse.splice @key_path, @from

      query_keys = [path1, path2].max_by(&:length)
      support_query_for_path index, query_keys, nil, all
    end

    private

    # Get the names of all required fields on this path
    # @return [Array<String>]
    def required_fields(index, query_keys, all)
      # Don't require selecting fields given in the WHERE clause or settings
      required_fields = index.hash_fields + index.order_fields
      required_fields += index.extra if all
      required_fields -= given_fields
      return [] if required_fields.empty?

      # Get the full name of each field to be used during selection
      required_fields.map! do |field|
        parent = query_keys.find_field_parent field
        next if parent.nil?

        "#{parent.name}.#{field.name}"
      end

      # These fields may not be in this part of path
      required_fields.delete_if(&:nil?)

      required_fields
    end

    # Find where the index path intersects the update path
    # and splice in the path of the where clause from the update
    # @return [KeyPath]
    def splice_path(source, target, from)
      if source.first.parent == from
        query_keys = KeyPath.new([from.id_fields.first])
      else
        query_keys = KeyPath.new(source.each_cons(2).take_while do |key, _|
          next true if key.instance_of?(Fields::IDField)
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

    def initialize(statement, model, group: nil, label: label)
      super :update, statement, model, group: group, label: label

      populate_conditions
      populate_settings

      freeze
    end

    # Specifies that updates require insertion
    def requires_insert?(_index)
      true
    end

    # Specifies that updates require deletion
    def requires_delete?(index)
      !(settings.map(&:field).to_set &
        (index.hash_fields + index.order_fields.to_set)).empty?
    end

    # Get the support queries for updating an index
    # @return [Array<SupportQuery>]
    def support_queries(index)
      # Get the updated fields and check if an update is necessary
      set_fields = settings.map(&:field).to_set

      # We only need to fetch all the fields if we're updating a key
      updated_key = !(set_fields &
                      (index.hash_fields + index.order_fields)).empty?

      updated_fields = set_fields & index.all_fields
      [support_query_for_fields(index, updated_fields, updated_key)].compact
    end

    # The condition fields are provided with the update
    # Note that we don't include the settings here because we
    # care about the previously existing values in the database
    def given_fields
      @conditions.each_value.map(&:field)
    end
  end

  # A representation of an insert in the workload
  class Insert < Statement
    include StatementConditions
    include StatementSettings
    include StatementSupportQuery

    alias entity from

    def initialize(statement, model, group: nil, label: label)
      super :insert, statement, model, group: group, label: label

      populate_settings
      fail InvalidStatementException, 'Must insert primary key' \
        unless @settings.map(&:field).include?(@from.id_fields.first)

      populate_conditions

      freeze
    end

    # Determine if this insert modifies an index
    def modifies_index?(index)
      return true if modifies_single_entity_index?(index)
      return false if index.path.length == 1
      return false unless index.path.entities.include? @from

      # Check if the index crosses any of the connection keys
      keys = @conditions.each_value.map(&:field)
      keys += keys.map(&:reverse)

      # We must be connecting on some component of the path
      # if the index is going to be modified by this insertion
      keys.count { |key| index.path.include?(key) } > 0
    end

    # Specifies that inserts require insertion
    def requires_insert?(_index)
      true
    end

    # Get the where clause for a support query over the given path
    # @return [String]
    def support_query_condition_for_path(keys, path)
      'WHERE ' + path.entries.map do |key|
        if keys.include?(key) ||
           (key.is_a?(Fields::ForeignKeyField) &&
            path.entities.include?(key.entity))
          # Find the ID for this entity in the path and include a predicate
          id = key.entity.id_fields.first
          "#{path.find_field_parent(id).name}.#{id.name} = ?"
        elsif path.entities.map { |e| e.id_fields.first }.include?(key)
          # Include the key for the entity being inserted
          "#{path.find_field_parent(key).name}.#{key.name} = ?"
        end
      end.compact.join(' AND ')
    end

    # Support queries are required for index insertion with connection
    # to select attributes of the other related entities
    # @return [Array<SupportQuery>]
    def support_queries(index)
      return [] unless modifies_index?(index) &&
                       !modifies_single_entity_index?(index)

      # Get the two path components
      entity_index = index.path.entities.index @from
      path1 = index.path[0..entity_index - 1]
      path2 = index.path[entity_index + 1..-1]

      # Group the connection keys into one of the two paths
      keys1 = []
      keys2 = []
      @conditions.each_value.map(&:field).each do |key|
        key = key.entity.id_fields.first

        keys1 << key if path1.include?(key)
        keys2 << key if path2.include?(key)
      end

      # Construct the two where clauses
      where1 = support_query_condition_for_path keys1, path1
      where2 = support_query_condition_for_path keys2, path2

      # Get the actual support queries
      [
        support_query_for_path(index, path1, where1, requires_insert?(index)),
        support_query_for_path(index, path2, where2, requires_insert?(index))
      ].compact
    end

    # The settings fields are provided with the insertion
    def given_fields
      @settings.map(&:field) + @conditions.each_value.map do |condition|
        condition.field.entity.id_fields.first
      end
    end

    private

    # Check if the insert modifies a single entity index
    # @return [Boolean]
    def modifies_single_entity_index?(index)
      !(@settings.map(&:field).to_set & index.all_fields).empty? &&
        index.path.length == 1 && index.path.first.parent == @from
    end

    # Populate conditions with the foreign key settings
    # @return [void]
    def populate_conditions
      connections = @tree[:connections] || []
      connections = connections.map do |connection|
        field = @from[connection[:target].to_s]
        value = connection[:target_pk]

        type = field.class.const_get 'TYPE'
        value = field.class.value_from_string(value.to_s) \
          unless type.nil? || value.nil?

        connection.delete :value
        Condition.new field, :'=', value
      end

      @conditions = Hash[connections.map do |connection|
        [connection.field.id, connection]
      end]
    end
  end

  # A representation of a delete in the workload
  class Delete < Statement
    include StatementConditions
    include StatementSupportQuery

    def initialize(statement, model, group: nil, label: label)
      super :delete, statement, model, group: group, label: label

      populate_conditions

      freeze
    end

    # Index contains the single entity to be deleted
    def modifies_index?(index)
      index.path.entities == [@from]
    end

    # Specifies that deletes require deletion
    def requires_delete?(_index)
      true
    end

    # Get the support queries for deleting from an index
    def support_queries(index)
      [support_query_for_fields(index, @from.fields)].compact
    end

    # The condition fields are provided with the deletion
    def given_fields
      @conditions.each_value.map(&:field)
    end
  end

  # Superclass for connect and disconnect statements
  class Connection < Statement
    include StatementSupportQuery

    attr_reader :source_pk, :target, :target_pk, :conditions
    alias source from

    # A connection modifies an index if the relationship is in the path
    def modifies_index?(index)
      index.path.include?(@target) || index.path.include?(@target.reverse)
    end

    # Get the support queries for updating an index
    def support_queries(index)
      return [] unless modifies_index?(index)

      # Get the key in the correct order
      reversed = !index.path.include?(@target)
      foreign_key = @target
      foreign_key = @target.reverse if reversed

      # Get the two path components
      entity_index = index.path.entities.index foreign_key.parent
      path1 = index.path[0..entity_index]
      path2 = index.path[entity_index + 1..-1].reverse

      # Construct the two where clauses
      where1 = support_query_condition_for_path path1, reversed
      where2 = support_query_condition_for_path path2, !reversed

      # Get the actual support queries
      [
        support_query_for_path(index, path1, where1, requires_insert?(index)),
        support_query_for_path(index, path2, where2, requires_insert?(index))
      ].compact
    end

    protected

    # Populate the keys and entities
    # @return [void]
    def populate_keys
      @source_pk = @tree[:source_pk]
      @target = @from.foreign_keys[@tree[:target].to_s]
      @target_pk = @tree[:target_pk]

      # Remove keys from the tree so we match on equality comparisons
      @tree.delete :source_pk
      @tree.delete :target_pk

      validate_keys

      # This is needed later when planning updates
      @eq_fields = [@target.parent.id_fields.first,
                    @target.entity.id_fields.first]

      populate_conditions
    end

    # The two key fields are provided with the connection
    def given_fields
      [@target.parent.id_fields.first, @target.entity.id_fields.first]
    end

    private

    # Validate the types of the primary keys
    # @return [void]
    def validate_keys
      # XXX Only works for non-composite PKs
      source_type = @from.id_fields.first.class.const_get 'TYPE'
      fail TypeError unless source_type.nil? || source_pk.is_a?(type)

      target_type = @target.class.const_get 'TYPE'
      fail TypeError unless target_type.nil? || target_pk.is_a?(type)
    end

    # Populate the conditions
    # @return [void]
    def populate_conditions
      source_id = @from.id_fields.first
      target_id = @target.entity.id_fields.first
      @conditions = {
        source_id.id => Condition.new(source_id, :'=', @source_pk),
        target_id.id => Condition.new(target_id, :'=', @target_pk)
      }
    end

    # Get the where clause for a support query over the given path
    # @return [String]
    def support_query_condition_for_path(path, reversed)
      key = (reversed ? target.entity : target.parent).id_fields.first
      path = path.reverse if path.entities.last != key.entity
      eq_key = path.entries[-1]
      if eq_key.is_a? Fields::ForeignKeyField
        where = "WHERE #{eq_key.name}.#{eq_key.entity.id_fields.first.name} = ?"
      else
        where = "WHERE #{eq_key.parent.name}." \
                "#{eq_key.parent.id_fields.first.name} = ?"
      end

      where
    end
  end

  # A representation of a connect in the workload
  class Connect < Connection
    def initialize(statement, model, group: nil, label: label)
      super :connect, statement, model, group: group, label: label
      fail InvalidStatementException, 'DISCONNECT parsed as CONNECT' \
        unless @text.split.first == 'CONNECT'
      populate_keys
      freeze
    end

    # Specifies that connections require insertion
    def requires_insert?(_index)
      true
    end
  end

  # A representation of a disconnect in the workload
  class Disconnect < Connection
    def initialize(statement, model, group: nil, label: label)
      super :connect, statement, model, group: group, label: label
      fail InvalidStatementException, 'CONNECT parsed as DISCONNECT' \
        unless @text.split.first == 'DISCONNECT'
      populate_keys
      freeze
    end

    # Specifies that disconnections require deletion
    def requires_delete?(_index)
      true
    end
  end

  # Thrown when something tries to parse an invalid statement
  class InvalidStatementException < StandardError
  end
end
