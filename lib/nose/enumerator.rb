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

      indexes_for_path(query.key_path.reverse, query.select,
                       eq, range) << query.materialize_view
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
      end.group_by { |index| [index.hash_fields, index.path] }

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

    # Produce all possible indices for a given path through the entity graph
    # which select the given fields and possibly allow equality/range filtering
    def indexes_for_path(path, select, eq, range)
      indexes = Set.new

      path.each_with_index do |_, i|
        path[i..-1].each_with_index do |_, j|
          j += i
          indexes += indexes_for_step path[i..j], select, eq, range
        end
      end

      indexes
    end

    # Get all possible index fields for entities on a path
    def index_choices(path, eq)
      path.entities.flat_map do |entity|
        # Get the fields for the entity and add in the IDs
        entity_fields = eq[entity] + [path.first]
        1.upto(entity_fields.count).flat_map do |n|
          entity_fields.permutation(n).to_a
        end
      end
    end

    # Get fields which should be included in an index for the given path
    def extra_choices(path_entities, select, eq, range)
      filter_choices = eq[path_entities.last] + range[path_entities.last]
      choices = [[]]

      # Include any fields which might be selected
      select_fields = select.select do |field|
        path_entities.include? field.parent
      end

      choices << select_fields unless select_fields.empty?
      choices << filter_choices unless filter_choices.empty?
      choices
    end

    # Get all possible indices which jump a given section in a query path
    def indexes_for_step(path, select, eq, range)
      @logger.debug "Enumerating indexes on path step #{path.map(&:name)}"

      index_choices = index_choices path, eq
      max_eq_fields = index_choices.map(&:length).max

      range_fields = path.entities.map { |entity| range[entity] }.reduce(&:+)
      order_choices = range_fields.prefixes.to_a << []
      extra_choices = extra_choices path.entities, select, eq, range

      # Generate all possible indices based on the field choices
      choices = index_choices.product(extra_choices)
      choices.map do |index, extra|
        indexes = []

        order_choices.each do |order|
          # Append the primary key of the last entity in the path if needed
          order += path.entities.flat_map(&:id_fields) - (index + order)

          # Skip indices with only a hash component
          index_extra = extra - (index + order)

          next if order.empty? && index_extra.empty?

          new_index = generate_index index, order, index_extra, path
          indexes << new_index unless new_index.nil?

          # Partition into the ordering portion
          if index.length == max_eq_fields
            index.partitions.each do |index_prefix, order_prefix|
              new_index = generate_index index_prefix, order_prefix + order,
                                         extra, path
              indexes << new_index unless new_index.nil?
            end
          end
        end

        indexes
      end.inject([], &:+).flatten
    end

    # Generate a new index and ignore if invalid
    def generate_index(hash, order, extra, path)
      begin
        index = Index.new hash, order, extra, path
        @logger.debug "Enumerated #{index.inspect}"
      rescue InvalidIndexException
        # This combination of fields is not valid, that's ok
        index = nil
      end

      index
    end
  end
end
