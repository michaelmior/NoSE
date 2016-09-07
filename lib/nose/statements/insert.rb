# frozen_string_literal: true

module NoSE
  # A representation of an insert in the workload
  class Insert < Statement
    include StatementConditions
    include StatementSettings
    include StatementSupportQuery

    def initialize(params, text, group: nil, label: nil)
      super params, text, group: group, label: label

      @settings = params[:settings]
      fail InvalidStatementException, 'Must insert primary key' \
        unless @settings.map(&:field).include?(entity.id_field)

      populate_conditions params
    end

    # Build a new insert from a provided parse tree
    # @return [Insert]
    def self.parse(tree, params, text, group: nil, label: nil)
      settings_from_tree tree, params
      conditions_from_tree tree, params

      Insert.new params, text, group: group, label: label
    end

    # Extract conditions from a parse tree
    # @return [Hash]
    def self.conditions_from_tree(tree, params)
      connections = tree[:connections] || []
      connections = connections.map do |connection|
        field = params[:entity][connection[:target].to_s]
        value = connection[:target_pk]

        type = field.class.const_get 'TYPE'
        value = field.class.value_from_string(value.to_s) \
          unless type.nil? || value.nil?

        connection.delete :value
        Condition.new field, :'=', value
      end

      params[:conditions] = Hash[connections.map do |connection|
        [connection.field.id, connection]
      end]
    end
    private_class_method :conditions_from_tree

    # Produce the SQL text corresponding to this insert
    # @return [String]
    def unparse
      insert = "INSERT INTO #{entity.name} "
      insert += settings_clause

      insert << ' AND CONNECT TO ' << @conditions.values.map do |condition|
        value = maybe_quote condition.value, condition.field
        "#{condition.field.name}(#{value})"
      end.join(', ') unless @conditions.empty?

      insert
    end

    def ==(other)
      other.is_a?(Insert) &&
        @graph == other.graph &&
        entity == other.entity &&
        @settings == other.settings &&
        @conditions == other.conditions
    end
    alias eql? ==

    def hash
      @hash ||= [@graph, entity, @settings, @conditions].hash
    end

    # Determine if this insert modifies an index
    def modifies_index?(index)
      return true if modifies_single_entity_index?(index)
      return false if index.graph.size == 1
      return false unless index.graph.entities.include? entity

      # Check if the index crosses all of the connection keys
      keys = @conditions.each_value.map(&:field)
      index.graph.keys_from_entity(entity).all? { |k| keys.include? k }
    end

    # Specifies that inserts require insertion
    def requires_insert?(_index)
      true
    end

    # Support queries are required for index insertion with connection
    # to select attributes of the other related entities
    # @return [Array<SupportQuery>]
    def support_queries(index)
      return [] unless modifies_index?(index) &&
                       !modifies_single_entity_index?(index)

      # Get all fields which need to be selected by support queries
      select = index.all_fields -
               @settings.map(&:field).to_set -
               @conditions.each_value.map do |condition|
                 condition.field.entity.id_field
               end.to_set
      return [] if select.empty?

      index.graph.split(entity).map do |graph|
        support_fields = select.select do |field|
          graph.entities.include? field.parent
        end.to_set

        # Build conditions by traversing the foreign keys
        conditions = @conditions.each_value.map do |c|
          next unless graph.entities.include? c.field.entity

          Condition.new c.field.entity.id_field, c.operator, c.value
        end.compact
        conditions = Hash[conditions.map do |condition|
          [condition.field.id, condition]
        end]

        split_entity = split_entity graph, index.graph, entity
        build_support_query split_entity, index, graph, support_fields,
                            conditions
      end.compact
    end

    # The settings fields are provided with the insertion
    def given_fields
      @settings.map(&:field) + @conditions.each_value.map do |condition|
        condition.field.entity.id_field
      end
    end

    private

    # Check if the insert modifies a single entity index
    # @return [Boolean]
    def modifies_single_entity_index?(index)
      !(@settings.map(&:field).to_set & index.all_fields).empty? &&
        index.graph.size == 1 && index.graph.entities.first == entity
    end
  end
end
