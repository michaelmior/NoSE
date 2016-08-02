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

    # Index contains the single entity to be deleted
    def modifies_index?(index)
      index.path.entities == [entity]
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

      graphs = index.graph.size > 1 ? index.graph.split(entity, true) : []
      graphs << QueryGraph::Graph.new([entity])
      graphs.map do |graph|
        params = { graph: graph }
        params[:select] = select.select do |field|
          next false if graph.size > 1 && graph.entities.first == entity
          graph.entities.include? field.parent
        end.to_set
        next if params[:select].empty?

        params[:conditions] = @conditions.select do |_, c|
          index.graph.entities.include? c.field.parent
        end

        params[:key_path] = params[:graph].longest_path
        params[:entity] = params[:key_path].first.parent

        support_query = SupportQuery.new params, nil, group: @group
        support_query.instance_variable_set :@statement, self
        support_query.instance_variable_set :@index, index
        support_query.instance_variable_set :@comment, (hash ^ index.hash).to_s
        support_query.hash
        support_query.freeze
      end.compact
    end

    # The condition fields are provided with the deletion
    def given_fields
      @conditions.each_value.map(&:field)
    end
  end
end
