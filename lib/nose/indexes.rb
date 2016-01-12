module NoSE
  # A representation of materialized views over fields in an entity
  class Index
    attr_reader :hash_fields, :order_fields, :extra, :all_fields, :path,
                :entries, :entry_size, :size, :hash_count, :per_hash_count

    def initialize(hash_fields, order_fields, extra, path, saved_key = nil)
      @hash_fields = hash_fields.to_set
      @order_fields = order_fields - hash_fields.to_a
      @extra = extra.to_set - @hash_fields - @order_fields.to_set
      @all_fields = @hash_fields + order_fields.to_set + @extra
      @path = path.is_a?(KeyPath) ? path : KeyPath.new(path)
      @key = saved_key

      validate_index

      # Initialize the hash function and freeze ourselves
      hash
      key
      calculate_size
      freeze
    end

    # A simple key which uniquely identifies the index
    def key
      # Temporarily set an empty key
      if @key.nil?
        join_fields = lambda do |fields|
          fields.map { |field| "#{field.parent.name}.#{field.name}" }.join ', '
        end

        keystr = join_fields.call @hash_fields
        keystr += ' ' + join_fields.call(@order_fields)
        keystr += ' ' + join_fields.call(@extra)
        keystr += ' ' + @path.map(&:name).join(', ')

        @key = "i#{Zlib.crc32 keystr}"
      end

      @key
    end

    # :nocov:
    def to_color
      hash_names = @hash_fields.map(&:inspect)
      order_names = @order_fields.map(&:inspect)
      extra_names = @extra.map(&:inspect)
      '[magenta]' + @key + '[/] ' \
      '[' + hash_names.join(', ') + ']' \
        '[' + order_names.join(', ') + '] ' \
        '→ [' + extra_names.join(', ') + ']' \
        " [yellow]$#{size}[/]" \
        " [magenta]#{@path.map(&:name).join(', ')}[/]"
    end
    # :nocov:

    # Two indices are equal if they contain the same fields
    # @return [Boolean]
    def ==(other)
      hash == other.hash
    end
    alias_method :eql?, :==

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
      @hash_fields == entity.id_fields.to_set && @path.length == 1
    end

    # Check if this index maps from the primary
    # key to fields from  a single index
    # @return [Boolean]
    def identity?
      identity_for? @path.entities.first
    end

    # Check if the index contains a given field
    # @return [Boolean]
    def contains_field?(field)
      @all_fields.include? field
    end

    # Create a new range over the entities traversed by an index using
    # the numerical indices into a list of entities
    def entity_range(entities)
      indexes = @path.entities.map do |entity|
        entities.index entity
      end.compact

      indexes.empty? ? (nil..nil) : Range.new(*indexes.minmax)
    end

    private

    # Ensure this index is valid
    def validate_index
      fail InvalidIndexException, 'hash fields cannot be empty' \
        if @hash_fields.empty?

      fail InvalidIndexException, 'must have fields other than hash fields' \
        if @order_fields.empty? && @extra.empty?

      fail InvalidIndexException, 'hash fields can only involve one entity' \
        if @hash_fields.map(&:parent).to_set.size > 1

      entities = @all_fields.map(&:parent).to_set
      fail InvalidIndexException, 'invalid path for index fields' \
        unless entities.include?(path.entities.first) &&
               entities.include?(path.entities.last)

      # We must have the primary keys of the all entities on the path
      fail InvalidIndexException, 'missing path entity keys' \
        unless @path.entities.flat_map(&:id_fields).all?(
          &(@hash_fields + @order_fields).method(:include?))
    end

    # Precalculate the size of the index
    def calculate_size
      @hash_count = @hash_fields.product_by(&:cardinality)

      # XXX This only works if foreign keys span all possible keys
      #     Take the maximum possible count at each join and multiply
      @entries = @path.entities.map(&:count).max
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
                [id_fields.first], name
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
      eq = @eq_fields.select { |field| field.parent == hash_entity }
      eq = @longest_entity_path.last.id_fields if eq.empty?
      order_fields = materialize_view_order(hash_entity) - eq

      Index.new(eq, order_fields,
                all_fields - (@eq_fields + @order).to_set,
                @key_path.reverse)
    end

    # Get the ordered keys for a materialized view
    def materialize_view_order(hash_entity)
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
