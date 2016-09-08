# frozen_string_literal: true

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

      range = if query.range_field.nil?
                query.order
              else
                [query.range_field] + query.order
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
    def indexes_for_workload(additional_indexes = [], by_id_graph = false)
      queries = @workload.queries
      indexes = Parallel.map(queries) do |query|
        indexes_for_query(query).to_a
      end.inject(additional_indexes, &:+)

      # Add indexes generated for support queries
      supporting = support_indexes indexes, by_id_graph
      supporting += support_indexes supporting, by_id_graph
      indexes += supporting

      # Deduplicate indexes, combine them and deduplicate again
      indexes.uniq!
      combine_indexes indexes
      indexes.uniq!

      @logger.debug do
        "Indexes for workload:\n" + indexes.map.with_index do |index, i|
          "#{i} #{index.inspect}"
        end.join("\n")
      end

      indexes
    end

    private

    # Produce the indexes necessary for support queries for these indexes
    # @return [Array<Index>]
    def support_indexes(indexes, by_id_graph)
      # If indexes are grouped by ID graph, convert them before updating
      # since other updates will be managed automatically by index maintenance
      indexes = indexes.map(&:to_id_graph).uniq if by_id_graph

      # Collect all possible support queries
      queries = indexes.flat_map do |index|
        @workload.updates.flat_map do |update|
          update.support_queries(index)
        end
      end

      # Enumerate indexes for each support query
      queries.uniq!
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
        [index.hash_fields, index.graph]
      end

      no_order_indexes.each do |(hash_fields, graph), hash_indexes|
        extra_choices = hash_indexes.map(&:extra).uniq

        # XXX More combos?
        combos = extra_choices.combination(2)

        combos.map do |combo|
          indexes << Index.new(hash_fields, [], combo.inject(Set.new, &:+),
                               graph)
          @logger.debug "Enumerated combined index #{indexes.last.inspect}"
        end
      end
    end

    # Get all possible choices of fields to use for equality
    # @return [Array<Array>]
    def eq_choices(graph, eq)
      entity_choices = graph.entities.flat_map do |entity|
        # Get the fields for the entity and add in the IDs
        entity_fields = eq[entity] << entity.id_field
        entity_fields.uniq!
        1.upto(entity_fields.count).flat_map do |n|
          entity_fields.permutation(n).to_a
        end
      end

      2.upto(graph.entities.length).flat_map do |n|
        entity_choices.permutation(n).map(&:flatten).to_a
      end + entity_choices
    end

    # Get fields which should be included in an index for the given graph
    # @return [Array<Array>]
    def extra_choices(graph, select, eq, range)
      choices = eq.values + range.values << select.to_a

      choices.each do |choice|
        choice.select { |field| graph.entities.include?(field.parent) }
      end

      choices.reject(&:empty?) << []
    end

    # Get all possible indices which jump a given piece of a query graph
    # @return [Array<Index>]
    def indexes_for_graph(graph, select, eq, range)
      eq_choices = eq_choices graph, eq
      range_fields = graph.entities.map { |entity| range[entity] }.reduce(&:+)
      range_fields.uniq!
      order_choices = range_fields.prefixes.flat_map do |fields|
        fields.permutation.to_a
      end.uniq << []
      extra_choices = extra_choices graph, select, eq, range
      extra_choices = 1.upto(extra_choices.length).flat_map do |n|
        extra_choices.combination(n).map(&:flatten).map(&:uniq)
      end.uniq

      # Generate all possible indices based on the field choices
      choices = eq_choices.product(extra_choices)
      indexes = choices.map! do |index, extra|
        indexes = []

        order_choices.each do |order|
          # Append the primary key of the entities in the graph if needed
          order += graph.entities.sort_by(&:name).map(&:id_field) -
                   (index + order)

          # Partition into the ordering portion
          index.partitions.each do |index_prefix, order_prefix|
            hash_fields = index_prefix.take_while do |field|
              field.parent == index.first.parent
            end
            order_fields = index_prefix[hash_fields.length..-1] + \
                           order_prefix + order
            extra_fields = extra - hash_fields - order_fields
            next if order_fields.empty? && extra_fields.empty?

            new_index = generate_index hash_fields, order_fields, extra_fields,
                                       graph
            indexes << new_index unless new_index.nil?
          end
        end

        indexes
      end.inject([], &:+)
      indexes.flatten!

      indexes
    end

    # Generate a new index and ignore if invalid
    # @return [Index]
    def generate_index(hash, order, extra, graph)
      begin
        index = Index.new hash, order.uniq, extra, graph
        @logger.debug { "Enumerated #{index.inspect}" }
      rescue InvalidIndexException
        # This combination of fields is not valid, that's ok
        index = nil
      end

      index
    end
  end
end
