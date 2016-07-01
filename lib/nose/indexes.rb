module NoSE
  # A representation of materialized views over fields in an entity
  class Index
    attr_reader :hash_fields, :order_fields, :extra, :all_fields, :path,
                :entries, :entry_size, :size, :hash_count, :per_hash_count,
                :graph

    def initialize(hash_fields, order_fields, extra, graph, saved_key = nil)
      @hash_fields = hash_fields.to_set
      @order_fields = order_fields - hash_fields.to_a
      @extra = extra.to_set - @hash_fields - @order_fields.to_set
      @all_fields = @hash_fields + order_fields.to_set + @extra

      validate_hash_fields

      @graph = graph
      @path = graph.to_path(hash_fields.first.parent)

      validate_path

      build_hash saved_key
    end

    # A simple key which uniquely identifies the index
    # @return [String]
    def key
      return @key unless @key.nil?

      # Join all of the fields in the key as well as entities on
      # the path to create a string which is hashed to give the key
      join_fields = lambda do |fields|
        fields.map { |field| "#{field.parent.name}.#{field.name}" }.join ', '
      end

      keystr = [@hash_fields, @order_fields, @extra].map(&join_fields).join ' '
      keystr += ' ' + @path.map(&:name).join(', ')

      @key = "i#{Zlib.crc32 keystr}"
    end

    # Look up a field in the index based on its ID
    # @return [Fields::Field]
    def [](field_id)
      @all_fields.find { |field| field.id == field_id }
    end

    # :nocov:
    def to_color
      fields = [@hash_fields, @order_fields, @extra].map do |field_group|
        '[' + field_group.map(&:inspect).join(', ') + ']'
      end

      "[magenta]#{key}[/] #{fields[0]} #{fields[1]} → #{fields[2]}" \
        " [yellow]$#{size}[/]" \
        " [magenta]#{@path.map(&:name).join(', ')}[/]"
    end
    # :nocov:

    # Two indices are equal if they contain the same fields
    # @return [Boolean]
    def ==(other)
      hash == other.hash
    end
    alias eql? ==

    # Hash based on the fields, their keys, and the path
    # @return [String]
    def hash_str
      @hash_str ||= [
        @hash_fields.map(&:id).sort,
        @order_fields.map(&:id),
        @extra.to_set.map(&:id).sort,
        @path.map(&:id)
      ].to_s
    end

    def hash
      @hash ||= Zlib.crc32 hash_str
    end

    # Check if this index is a mapping from the key of the given entity
    # @see Entity#id_fields
    # @return [Boolean]
    def identity_for?(entity)
      @hash_fields == entity.id_fields.to_set && @graph.nodes.size == 1
    end

    # Check if this index maps from the primary
    # key to fields from  a single index
    # @return [Boolean]
    def identity?
      !@graph.root.nil? && identity_for?(@graph.root.entity)
    end

    # Check if the index contains a given field
    # @return [Boolean]
    def contains_field?(field)
      @all_fields.include? field
    end

    # Create a new range over the entities traversed by an index using
    # the numerical indices into a list of entities
    # @return [Range]
    def entity_range(entities)
      indexes = @path.entities.map do |entity|
        entities.index entity
      end.compact

      indexes.empty? ? (nil..nil) : Range.new(*indexes.minmax)
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

    # Ensure an index and its fields correspond to a valid path
    # @return [void]
    def validate_path
      validate_path_entities
      validate_path_keys
    end

    # Ensure the path of the index is valid
    # @return [void]
    def validate_path_entities
      entities = @all_fields.map(&:parent).to_set
      fail InvalidIndexException, 'path entities do match index' \
        unless entities == path.entities.to_set
    end

    # We must have the primary keys of the all entities in the graph
    # @return [void]
    def validate_path_keys
      fail InvalidIndexException, 'missing path entity keys' \
        unless @graph.entities.flat_map(&:id_fields).all?(
          &(@hash_fields + @order_fields).method(:include?))
    end

    # Precalculate the size of the index
    # @return [void]
    def calculate_size
      @hash_count = @hash_fields.product_by(&:cardinality)

      # XXX This only works if foreign keys span all possible keys
      #     Take the maximum possible count at each join and multiply
      @entries = @graph.entities.map(&:count).max
      @per_hash_count = (@entries * 1.0 / @hash_count).round

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
      Index.new id_fields, [], fields.values - id_fields,
                QueryGraph::Graph.from_path([id_fields.first]), name
    end
  end

  # Allow statements to materialize views
  class Statement
    # Construct an index which acts as a materialized view for a query
    # @return [Index]
    def materialize_view
      # We must have hash fields, so if there are no equality
      # predicates, use the ID at the end of the query path instead
      hash_entity = @longest_entity_path.detect do |entity|
        @eq_fields.any? { |field| field.parent == entity }
      end

      eq = materialized_view_eq hash_entity
      order_fields = materialized_view_order(hash_entity) - eq

      Index.new(eq, order_fields,
                all_fields - (@eq_fields + @order).to_set,
                QueryGraph::Graph.from_path(@key_path.reverse))
    end

    private

    # Get the fields used as parition keys for a materialized view
    # based over a given entity
    # @return [Array<Fields::Field>]
    def materialized_view_eq(hash_entity)
      eq = @eq_fields.select { |field| field.parent == hash_entity }
      eq = @longest_entity_path.last.id_fields if eq.empty?

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
      order_fields += @longest_entity_path.reverse_each.flat_map(&:id_fields)

      order_fields.uniq
    end
  end
end
