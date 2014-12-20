module NoSE
  # A representation of materialized views over fields in an entity
  class Index
    attr_reader :hash_fields, :order_fields, :extra, :all_fields, :path,
                :entry_size, :size

    def initialize(hash_fields, order_fields, extra, path, saved_key = nil)
      @hash_fields = hash_fields.to_set
      @order_fields = order_fields - hash_fields.to_a
      @extra = extra.to_set - @hash_fields - @order_fields.to_set
      @all_fields = @hash_fields + order_fields.to_set + @extra
      @path = path
      @key = saved_key

      fail InvalidIndexException, 'hash fields cannot be empty' \
        if @hash_fields.empty?

      entities = @all_fields.map(&:parent).to_set
      fail InvalidIndexException, 'invalid path for index fields' \
        unless entities.include?(path.first) && entities.include?(path.last)

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

    def state
      {
        hash_fields: @hash_fields.map(&:state),
        order_fields: @order_fields.map(&:state),
        extra: @extra.map(&:state),
        path: @path.map(&:state)
      }
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
    # @return [Fixnum]
    def hash
      @hash ||= [@hash_fields, @order_fields, @extra.to_set, @path].hash
    end

    # Precalculate the size of the index
    def calculate_size
      @entry_size = @all_fields.map(&:size).inject(0, :+)
      num_entries = @hash_fields.map(&:cardinality).inject(1, &:*)
      @size =  Cardinality.new_cardinality(@path.first.count,
                                           @hash_fields,
                                           nil,
                                           @path) * @entry_size * num_entries
    end

    # Check if this index is a mapping from the key of the given entity
    # @see Entity#id_fields
    # @return [Boolean]
    def identity_for?(entity)
      @hash_fields == entity.id_fields.to_set
    end

    # Check if the index contains a given field
    # @return [Boolean]
    def contains_field?(field)
      @all_fields.include? field
    end

    # Create a new range over the entities traversed by an index using
    # the numerical indices into a list of entities
    def entity_range(entities)
      Range.new(*(@path.map do |entity|
        entities.index entity.name
      end).minmax) rescue (nil..nil)
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
      Index.new(id_fields, [], fields.values - id_fields, [self], name)
    end
  end

  # Allow statements to materialize views
  class Statement
    # Construct an index which acts as a materialized view for a query
    # @return [Index]
    def materialize_view
      order_fields = @order
      if @range_field && !@order.include?(@range_field)
        order_fields << @range_field
      end

      # We must have hash fields, so if there are no equality
      # predicates, use the ID at the end of the query path instead
      eq = @eq_fields
      eq = @longest_entity_path.last.id_fields if @eq_fields.empty?

      NoSE::Index.new(eq, order_fields,
                      all_fields - (@eq_fields + @order).to_set,
                      @longest_entity_path.reverse)
    end
  end
end
