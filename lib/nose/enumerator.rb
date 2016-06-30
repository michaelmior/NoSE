require 'logging'

module NoSE
  # Produces potential indices to be used in schemas
  class IndexEnumerator
    def initialize(workload)
      @logger = Logging.logger['nose::enumerator']

      @workload = workload
    end

    # Produce all possible indices for a given query
    # @return [Array<Index>]
    def indexes_for_query(query)
      @logger.debug "Enumerating indexes for query #{query.text}"

      if query.range_field.nil?
        range = query.order
      else
        range = [query.range_field] + query.order
      end

      eq = query.eq_fields.group_by(&:parent)
      eq.default_proc = ->(*) { [] }

      range = range.group_by(&:parent)
      range.default_proc = ->(*) { [] }

      query.graph.subgraphs.flat_map do |graph|
        indexes_for_graph graph, query.select, eq, range
      end.uniq << query.materialize_view
    end

    # Produce all possible indices for a given workload
    # @return [Set<Index>]
    def indexes_for_workload(additional_indexes = [])
      queries = @workload.queries
      indexes = Parallel.map(queries) do |query|
        indexes_for_query(query).to_a
      end.inject(additional_indexes, &:+)

      # Add indexes generated for support queries
      supporting = support_indexes indexes
      supporting += support_indexes supporting
      indexes += supporting

      # Deduplicate indexes, combine them and deduplicate again
      indexes.uniq!
      combine_indexes indexes
      indexes.uniq!

      @logger.debug do
        "Indexes for workload:\n" + indexes.each_with_index.map do |index, i|
          "#{i} #{index.inspect}"
        end.join("\n")
      end

      indexes
    end

    private

    # Produce the indexes necessary for support queries for these indexes
    # @return [Array<Index>]
    def support_indexes(indexes)
      # Collect all possible support queries
      queries = indexes.flat_map do |index|
        @workload.updates.flat_map do |update|
          update.support_queries(index)
        end
      end

      # Enumerate indexes for each support query
      queries.uniq!(&:text)
      queries.flat_map do |query|
        indexes_for_query(query).to_a
      end
    end

    # Combine the data of indices based on matching hash fields
    def combine_indexes(indexes)
      no_order_indexes = indexes.select do |index|
        index.order_fields.empty?
      end
      no_order_indexes = no_order_indexes.group_by do |index|
        [index.hash_fields, index.path]
      end

      no_order_indexes.each do |(hash_fields, path), hash_indexes|
        extra_choices = hash_indexes.map(&:extra).uniq

        # XXX More combos?
        combos = extra_choices.combination(2)

        combos.map do |combo|
          indexes << Index.new(hash_fields, [], combo.inject(Set.new, &:+),
                               path)
          @logger.debug "Enumerated combined index #{indexes.last.inspect}"
        end
      end
    end

    # Get all possible index fields for entities on a path
    # @return [Array<Array>]
    def index_choices(graph, eq)
      graph.entities.flat_map do |entity|
        # Get the fields for the entity and add in the IDs
        entity_fields = eq[entity] + entity.id_fields
        1.upto(entity_fields.count).flat_map do |n|
          entity_fields.permutation(n).to_a
        end
      end
    end

    # Get fields which should be included in an index for the given graph
    # @return [Array<Array>]
    def extra_choices(graph, select, eq, range)
      filter_choices = eq[graph.root.entity] + range[graph.root.entity]
      choices = [[]]

      # Include any fields which might be selected
      select_fields = select.select do |field|
        graph.entities.include? field.parent
      end

      choices << select_fields unless select_fields.empty?
      choices << filter_choices unless filter_choices.empty?
      choices
    end

    # Get all possible indices which jump a given piece of a query graph
    # @return [Array<Index>]
    def indexes_for_graph(graph, select, eq, range)
      index_choices = index_choices graph, eq
      index_choices += index_choices.map(&:reverse)
      max_eq_fields = index_choices.max_by(&:length).length

      range_fields = graph.entities.map { |entity| range[entity] }.reduce(&:+)
      order_choices = range_fields.prefixes.flat_map do |fields|
        fields.permutation.to_a
      end.uniq << []
      extra_choices = extra_choices graph, select, eq, range

      # Generate all possible indices based on the field choices
      choices = index_choices.product(extra_choices)
      choices.map do |index, extra|
        indexes = []

        order_choices.each do |order|
          # Append the primary key of the last entity in the path if needed
          order += graph.entities.flat_map(&:id_fields) - (index + order)

          # Skip indices with only a hash component
          index_extra = extra - (index + order)

          next if order.empty? && index_extra.empty?

          new_index = generate_index index, order, index_extra, graph
          indexes << new_index unless new_index.nil?

          # Partition into the ordering portion
          next unless index.length == max_eq_fields
          index.partitions.each do |index_prefix, order_prefix|
            new_index = generate_index index_prefix, order_prefix + order,
                                       extra, graph
            indexes << new_index unless new_index.nil?
          end
        end

        indexes
      end.inject([], &:+).flatten
    end

    # Generate a new index and ignore if invalid
    # @return [Index]
    def generate_index(hash, order, extra, graph)
      begin
        index = Index.new hash, order, extra, graph.to_path(hash.first.parent)
        @logger.debug "Enumerated #{index.inspect}"
      rescue InvalidIndexException, InvalidPathException
        # This combination of fields is not valid, that's ok
        index = nil
      end

      index
    end
  end
end
