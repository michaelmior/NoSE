module Sadvisor
  # Produces potential indices to be used in schemas
  class IndexEnumerator
    def initialize(workload)
      @workload = workload
    end

    # Produce all possible indices for a given query
    # @return [Array<Index>]
    def indexes_for_query(query)
      if query.range_field.nil?
        range = query.order
      else
        range = [query.range_field] + query.order
      end

      eq = query.eq_fields.group_by(&:parent)
      eq.default_proc = ->(*) { [] }

      range = range.group_by(&:parent)
      range.default_proc = ->(*) { [] }

      indexes_for_path query.longest_entity_path.reverse, query.select,
                       eq, range
    end

    # Produce all possible indices for a given workload
    # @return [Set<Index>]
    def indexes_for_workload
      indexes = Parallel.map(@workload.queries) do |query|
        indexes_for_query(query).to_a << query.materialize_view
      end.inject([], &:+)

      combine_indexes indexes
      remove_base_indexes indexes

      indexes.to_set
    end

    private

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
        end
      end
    end

    # Exclude things covered by the base indices
    def remove_base_indexes(indexes)
      indexes.reject! do |index|
        index.path.length == 1 &&
        index.hash_fields.to_set == index.path.first.id_fields.to_set &&
        index.order_fields.empty?
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

    # Get all possible index fields which jump a path with a set of filters
    def index_choices(path, eq)
      eq_fields = path.map { |entity| eq[entity] }.compact.flatten

      # Add the ID fields of the entity on the head of the path
      eq_fields += path.first.id_fields

      eq_choices = 1.upto(eq_fields.count).map do |n|
        eq_fields.permutation(n).to_a
      end.inject([], &:+).reject(&:empty?)

      eq_choices
    end

    # Get fields which should be included in an index for the given path
    def extra_choices(path, select, eq, range)
      filter_choices = eq[path.last] + range[path.last]
      choices = [path.last.id_fields]
      choices << select if path.include? select.first.parent
      choices << filter_choices unless filter_choices.empty?
      choices
    end

    # Get all possible indices which jump a given section in a query path
    def indexes_for_step(path, select, eq, range)
      index_choices = index_choices path, eq
      max_eq_fields = index_choices.map(&:length).max

      range_fields = path.map { |entity| range[entity] }.reduce(&:+)
      order_choices = range_fields.prefixes.to_a << []

      extra_choices = extra_choices path, select, eq, range

      # Generate all possible indices based on the field choices
      choices = index_choices.product(extra_choices)
      choices.map do |index, extra|
        indexes = []

        order_choices.each do |order|
          # Skip indices which will be in the base schema
          next if path.length == 1 && index == path[0].id_fields \
                                   && order.length == 0

          index_extra = extra - (index + order)
          next if order.empty? && index_extra.empty?

          begin
            indexes << Index.new(index, order, index_extra, path)
          rescue InvalidIndexException
            # This combination of fields is not valid, that's ok
            nil
          end

          # Partition into the ordering portion
          if index.length == max_eq_fields
            index.partitions.each do |index_prefix, order_prefix|
              begin
                indexes << Index.new(index_prefix, order_prefix + order,
                                     extra, path)
              rescue InvalidIndexException
                # This combination of fields is not valid, that's ok
                nil
              end
            end
          end
        end

        indexes
      end.inject([], &:+).flatten
    end
  end
end
