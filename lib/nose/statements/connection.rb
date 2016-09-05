# frozen_string_literal: true

module NoSE
  # Superclass for connect and disconnect statements
  class Connection < Statement
    include StatementSupportQuery

    attr_reader :source_pk, :target, :target_pk, :conditions
    alias source entity

    def initialize(params, text, group: nil, label: nil)
      super params, text, group: group, label: label
      fail InvalidStatementException, 'Incorrect connection initialization' \
        unless text.split.first == self.class.name.split('::').last.upcase

      populate_conditions params
    end

    # Build a new disconnect from a provided parse tree
    # @return [Connection]
    def self.parse(tree, params, text, group: nil, label: nil)
      keys_from_tree tree, params

      new params, text, group: group, label: label
    end

    # @return[void]
    def self.keys_from_tree(tree, params)
      params[:source_pk] = tree[:source_pk]
      params[:target] = params[:entity].foreign_keys[tree[:target].to_s]
      params[:target_pk] = tree[:target_pk]
    end

    # Produce the SQL text corresponding to this connection
    # @return [String]
    def unparse
      "CONNECT #{source.name}(\"#{source_pk}\") TO " \
        "#{target.name}(\"#{target_pk}\")"
    end

    def ==(other)
      self.class == other.class &&
        @graph == other.graph &&
        @source == other.source &&
        @target == other.target &&
        @conditions == other.conditions
    end
    alias eql? ==

    def hash
      @hash ||= [@graph, @source, @target, @conditions].hash
    end

    # A connection modifies an index if the relationship is in the path
    def modifies_index?(index)
      index.path.include?(@target) || index.path.include?(@target.reverse)
    end

    # Get the support queries for updating an index
    def support_queries(index)
      return [] unless modifies_index?(index)

      select = index.all_fields - @conditions.each_value.map(&:field).to_set
      return [] if select.empty?

      index.graph.split(entity).map do |graph|
        support_fields = select.select do |field|
          graph.entities.include? field.parent
        end.to_set
        conditions = @conditions.select do |_, c|
          graph.entities.include? c.field.parent
        end

        split_entity = split_entity graph, index.graph, entity
        build_support_query split_entity, index, graph, support_fields,
                            conditions
      end.compact
    end

    protected

    # The two key fields are provided with the connection
    def given_fields
      [@target.parent.id_field, @target.entity.id_field]
    end

    private

    # Validate the types of the primary keys
    # @return [void]
    def validate_keys
      # XXX Only works for non-composite PKs
      source_type = source.id_field.class.const_get 'TYPE'
      fail TypeError unless source_type.nil? || source_pk.is_a?(type)

      target_type = @target.class.const_get 'TYPE'
      fail TypeError unless target_type.nil? || target_pk.is_a?(type)
    end

    # Populate the list of condition objects
    # @return [void]
    def populate_conditions(params)
      @source_pk = params[:source_pk]
      @target = params[:target]
      @target_pk = params[:target_pk]

      validate_keys

      # This is needed later when planning updates
      @eq_fields = [@target.parent.id_field,
                    @target.entity.id_field]

      source_id = source.id_field
      target_id = @target.entity.id_field
      @conditions = {
        source_id.id => Condition.new(source_id, :'=', @source_pk),
        target_id.id => Condition.new(target_id, :'=', @target_pk)
      }
    end

    # Get the where clause for a support query over the given path
    # @return [String]
    def support_query_condition_for_path(path, reversed)
      key = (reversed ? target.entity : target.parent).id_field
      path = path.reverse if path.entities.last != key.entity
      eq_key = path.entries[-1]
      if eq_key.is_a? Fields::ForeignKeyField
        where = "WHERE #{eq_key.name}.#{eq_key.entity.id_field.name} = ?"
      else
        where = "WHERE #{eq_key.parent.name}." \
                "#{eq_key.parent.id_field.name} = ?"
      end

      where
    end
  end

  # A representation of a connect in the workload
  class Connect < Connection
    # Specifies that connections require insertion
    def requires_insert?(_index)
      true
    end
  end

  # A representation of a disconnect in the workload
  class Disconnect < Connection
    # Produce the SQL text corresponding to this disconnection
    # @return [String]
    def unparse
      "DISCONNECT #{source.name}(\"#{source_pk}\") FROM " \
        "#{target.name}(\"#{target_pk}\")"
    end

    # Specifies that disconnections require deletion
    def requires_delete?(_index)
      true
    end
  end
end
