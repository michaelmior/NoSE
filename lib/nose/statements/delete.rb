module NoSE
  # A representation of a delete in the workload
  class Delete < Statement
    include StatementConditions
    include StatementSupportQuery

    def initialize(params, text, group: nil, label: nil)
      super params, text, group: group, label: label

      populate_conditions params
    end

    # Build a new delete from a provided parse tree
    # @return [Delete]
    def self.parse(tree, params, text, group: nil, label: nil)
      conditions_from_tree tree, params

      Delete.new params, text, group: group, label: label
    end

    # Produce the SQL text corresponding to this delete
    # @return [String]
    def unparse
      delete = "DELETE #{entity.name} "
      delete += "FROM #{from_path @key_path}"
      delete += where_clause

      delete
    end

    def ==(other)
      other.is_a?(Delete) &&
        @graph == other.graph &&
        entity == other.entity &&
        @conditions == other.conditions
    end
    alias eql? ==

    def hash
      @hash ||= [@graph, entity, @conditions].hash
    end

    # Index contains the entity to be deleted
    def modifies_index?(index)
      index.graph.entities.include? entity
    end

    # Specifies that deletes require deletion
    def requires_delete?(_index)
      true
    end

    # Get the support queries for deleting from an index
    def support_queries(index)
      return [] unless modifies_index? index
      select = (index.hash_fields + index.order_fields.to_set) -
               @conditions.each_value.map(&:field).to_set
      return [] if select.empty?

      support_queries = []

      graph = Marshal.load(Marshal.dump(@graph))
      params = { graph: graph }
      params[:select] = select.select do |field|
        field.parent == entity
      end.to_set
      params[:select] << entity.id_field \
        unless @conditions.each_value.map(&:field).include? entity.id_field
      params[:conditions] = Hash[@conditions.map { |k, v| [k.dup, v.dup] }]
      params[:key_path] = params[:graph].longest_path
      params[:entity] = params[:key_path].first.parent

      support_query = SupportQuery.new params, nil, group: @group
      support_query.instance_variable_set :@statement, self
      support_query.instance_variable_set :@index, index
      support_query.instance_variable_set :@comment, (hash ^ index.hash).to_s
      support_query.hash
      support_query.freeze

      support_queries << support_query unless params[:select].empty?

      support_queries + support_queries_for_entity(index, select)
    end

    # The condition fields are provided with the deletion
    def given_fields
      @conditions.each_value.map(&:field)
    end
  end
end
