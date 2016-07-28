module NoSE
  # A representation of a delete in the workload
  class Delete < Statement
    include StatementConditions
    include StatementSupportQuery

    def initialize(tree, params, text, group: nil, label: nil)
      super params, text, group: group, label: label

      populate_conditions params
    end

    # Build a new delete from a provided parse tree
    # @return [Delete]
    def self.parse(tree, params, text, group: nil, label: nil)
      conditions_from_tree tree, params

      Delete.new tree, params, text, group: group, label: label
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
      @hash ||= [@graph, entity, @conditions]
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
      [support_query_for_fields(index, entity.fields)].compact
    end

    # The condition fields are provided with the deletion
    def given_fields
      @conditions.each_value.map(&:field)
    end
  end
end
