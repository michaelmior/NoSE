# frozen_string_literal: true

module NoSE
  # A representation of a query in the workload
  class Query < Statement
    include StatementConditions

    attr_reader :select, :order, :limit

    def initialize(params, text, group: nil, label: nil)
      super params, text, group: group, label: label

      populate_conditions params
      @select = params[:select]
      @order = params[:order] || []

      fail InvalidStatementException, 'can\'t order by IDs' \
        if @order.any? { |f| f.is_a? Fields::IDField }

      if join_order.first != @key_path.entities.first
        @key_path = @key_path.reverse
      end

      fail InvalidStatementException, 'must have an equality predicate' \
        if @conditions.empty? || @conditions.values.all?(&:is_range)

      @limit = params[:limit]
    end

    # Build a new query from a provided parse tree
    # @return [Query]
    def self.parse(tree, params, text, group: nil, label: nil)
      conditions_from_tree tree, params
      fields_from_tree tree, params
      order_from_tree tree, params
      params[:limit] = tree[:limit].to_i if tree[:limit]

      new params, text, group: group, label: label
    end

    # Produce the SQL text corresponding to this query
    # @return [String]
    def unparse
      field_namer = -> (f) { field_path f }

      query = 'SELECT ' + @select.map(&field_namer).join(', ')
      query << " FROM #{from_path @graph.longest_path}"
      query << where_clause(field_namer)

      query << ' ORDER BY ' << @order.map(&field_namer).join(', ') \
        unless @order.empty?
      query << " LIMIT #{@limit}" unless @limit.nil?
      query << " -- #{@comment}" unless @comment.nil?

      query
    end

    def ==(other)
      other.is_a?(Query) &&
        @graph == other.graph &&
        @select == other.select &&
        @conditions == other.conditions &&
        @order == other.order &&
        @limit == other.limit &&
        @comment == other.comment
    end
    alias eql? ==

    def hash
      @hash ||= [@graph, @select, @conditions, @order, @limit, @comment].hash
    end

    # The order entities should be joined according to the query graph
    # @return [Array<Entity>]
    def join_order
      @graph.join_order(@eq_fields)
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

    # Extract fields to be selected from a parse tree
    # @return [Set<Field>]
    def self.fields_from_tree(tree, params)
      params[:select] = tree[:select].flat_map do |field|
        if field.last == '*'
          # Find the entity along the path
          entity = params[:key_path].entities[tree[:path].index(field.first)]
          entity.fields.values
        else
          field = add_field_with_prefix tree[:path], field, params

          fail InvalidStatementException, 'Foreign keys cannot be selected' \
            if field.is_a? Fields::ForeignKeyField

          field
        end
      end.to_set
    end
    private_class_method :fields_from_tree

    # Extract ordering fields from a parse tree
    # @return [Array<Field>]
    def self.order_from_tree(tree, params)
      return params[:order] = [] if tree[:order].nil?

      params[:order] = tree[:order][:fields].each_slice(2).map do |field|
        field = field.first if field.first.is_a?(Array)
        add_field_with_prefix tree[:path], field, params
      end
    end
    private_class_method :order_from_tree

    private

    def field_path(field)
      path = @graph.path_between @graph.longest_path.entities.first,
                                 field.parent
      path = path.drop_while { |k| @graph.longest_path.include? k } << path[-1]
      path = KeyPath.new(path) unless path.is_a?(KeyPath)

      from_path path, @graph.longest_path, field
    end
  end

  # A query required to support an update
  class SupportQuery < Query
    attr_reader :statement, :index, :entity

    def initialize(entity, params, text, group: nil, label: nil)
      super params, text, group: group, label: label

      @entity = entity
    end

    # Support queries must also have their statement and index checked
    def ==(other)
      other.is_a?(SupportQuery) && @statement == other.statement &&
        @index == other.index && @comment == other.comment
    end
    alias eql? ==

    def hash
      @hash ||= Zlib.crc32_combine super, @index.hash, @index.hash_str.length
    end

    # :nocov:
    def to_color
      super.to_color + ' for [magenta]' + @index.key + '[/]'
    end
    # :nocov:
  end
end
