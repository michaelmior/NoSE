# frozen_string_literal: true

module NoSE
  # A representation of materialized views over fields in an entity
  class Index
    attr_reader :hash_fields, :order_fields, :extra, :all_fields, :path,
                :entries, :entry_size, :size, :hash_count, :per_hash_count,
                :graph

    def initialize(hash_fields, order_fields, extra, graph,
                   saved_key: nil)
      order_set = order_fields.to_set
      @hash_fields = hash_fields.to_set
      @order_fields = order_fields.delete_if { |e| hash_fields.include? e }
      @extra = extra.to_set.delete_if do |e|
        @hash_fields.include?(e) || order_set.include?(e)
      end
      @all_fields = Set.new(@hash_fields).merge(order_set).merge(@extra)

      validate_hash_fields

      # Store whether this index is an identity
      @identity = @hash_fields == [
        @hash_fields.first.parent.id_field
      ].to_set && graph.nodes.size == 1

      @graph = graph
      @path = graph.longest_path
      @path = nil unless @path.length == graph.size

      validate_graph

      build_hash saved_key
    end

    # Check if this index maps from the primary key to fields from one entity
    # @return [Boolean]
    def identity?
      @identity
    end

    # A simple key which uniquely identifies the index
    # @return [String]
    def key
      @key ||= "i#{Zlib.crc32 hash_str}"
    end

    # Look up a field in the index based on its ID
    # @return [Fields::Field]
    def [](field_id)
      @all_fields.find { |field| field.id == field_id }
    end

    # Check if this index is an ID graph
    # @return [Boolean]
    def id_graph?
      @hash_fields.all?(&:primary_key?) && @order_fields.all?(&:primary_key)
    end

    # Produce an index with the same fields but keyed by entities in the graph
    def to_id_graph
      return self if id_graph?

      all_ids = (@hash_fields.to_a + @order_fields + @extra.to_a)
      all_ids.map! { |f| f.parent.id_field }.uniq!

      hash_fields = [all_ids.first]
      order_fields = all_ids[1..-1]
      extra = @all_fields - hash_fields - order_fields

      Index.new hash_fields, order_fields, extra, @graph
    end

    # :nocov:
    def to_color
      fields = [@hash_fields, @order_fields, @extra].map do |field_group|
        '[' + field_group.map(&:inspect).join(', ') + ']'
      end

      "[magenta]#{key}[/] #{fields[0]} #{fields[1]} → #{fields[2]}" \
        " [yellow]$#{size}[/]" \
        " [magenta]#{@graph.inspect}[/]"
    end
    # :nocov:

    # Two indices are equal if they contain the same fields
    # @return [Boolean]
    def ==(other)
      hash == other.hash
    end
    alias eql? ==

    # Hash based on the fields, their keys, and the graph
    # @return [String]
    def hash_str
      @hash_str ||= [
        @hash_fields.map(&:id).sort!,
        @order_fields.map(&:id),
        @extra.map(&:id).sort!,
        @graph.unique_edges.map(&:canonical_params).sort!
      ].to_s.freeze
    end

    def hash
      @hash ||= Zlib.crc32 hash_str
    end

    # Check if the index contains a given field
    # @return [Boolean]
    def contains_field?(field)
      @all_fields.include? field
    end

    private

    # Initialize the hash function and freeze ourselves
    # @return [void]
    def build_hash(saved_key)
      @key = saved_key

      hash
      key
      calculate_size
      freeze
    end

    # Check for valid hash fields in an index
    # @return [void]
    def validate_hash_fields
      fail InvalidIndexException, 'hash fields cannot be empty' \
        if @hash_fields.empty?

      fail InvalidIndexException, 'hash fields can only involve one entity' \
        if @hash_fields.map(&:parent).to_set.size > 1
    end

    # Ensure an index is nonempty
    # @return [void]
    def validate_nonempty
      fail InvalidIndexException, 'must have fields other than hash fields' \
        if @order_fields.empty? && @extra.empty?
    end

    # Ensure an index and its fields correspond to a valid graph
    # @return [void]
    def validate_graph
      validate_graph_entities
      validate_graph_keys
    end

    # Ensure the graph of the index is valid
    # @return [void]
    def validate_graph_entities
      entities = @all_fields.map(&:parent).to_set
      fail InvalidIndexException, 'graph entities do match index' \
        unless entities == @graph.entities.to_set
    end

    # We must have the primary keys of the all entities in the graph
    # @return [void]
    def validate_graph_keys
      fail InvalidIndexException, 'missing graph entity keys' \
        unless @graph.entities.map(&:id_field).all? do |field|
          @hash_fields.include?(field) || @order_fields.include?(field)
        end
    end

    # Precalculate the size of the index
    # @return [void]
    def calculate_size
      @hash_count = @hash_fields.product_by(&:cardinality)

      # XXX This only works if foreign keys span all possible keys
      #     Take the maximum possible count at each join and multiply
      @entries = @graph.entities.map(&:count).max
      @per_hash_count = (@entries * 1.0 / @hash_count)

      @entry_size = @all_fields.sum_by(&:size)
      @size = @entries * @entry_size
    end
  end

  # Thrown when something tries to create an invalid index
  class InvalidIndexException < StandardError
  end

  # Allow entities to create their own indices
  class Entity
    # Create a simple index which maps entity keys to other fields
    # @return [Index]
    def simple_index
      Index.new [id_field], [], fields.values - [id_field],
                QueryGraph::Graph.from_path([id_field]), saved_key: name
    end
  end

  # Allow statements to materialize views
  class Statement
    # Construct an index which acts as a materialized view for a query
    # @return [Index]
    def materialize_view
      eq = materialized_view_eq join_order.first
      order_fields = materialized_view_order(join_order.first) - eq

      Index.new(eq, order_fields,
                all_fields - (@eq_fields + @order).to_set, @graph)
    end

    private

    # Get the fields used as parition keys for a materialized view
    # based over a given entity
    # @return [Array<Fields::Field>]
    def materialized_view_eq(hash_entity)
      eq = @eq_fields.select { |field| field.parent == hash_entity }
      eq = [join_order.last.id_field] if eq.empty?

      eq
    end

    # Get the ordered keys for a materialized view
    # @return [Array<Fields::Field>]
    def materialized_view_order(hash_entity)
      # Start the ordered fields with the equality predicates
      # on other entities, followed by all of the attributes
      # used in ordering, then the range field
      order_fields = @eq_fields.select do |field|
        field.parent != hash_entity
      end + @order
      if @range_field && !@order.include?(@range_field)
        order_fields << @range_field
      end

      # Ensure we include IDs of the final entity
      order_fields += join_order.map(&:id_field)

      order_fields.uniq
    end
  end
end
