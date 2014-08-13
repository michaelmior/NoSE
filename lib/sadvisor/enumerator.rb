module Sadvisor
  # Produces potential indices to be used in schemas
  class IndexEnumerator
    def initialize(workload)
      @workload = workload
    end

    # Produce all possible indices for a given query
    # @return [Array<Index>]
    def indexes_for_query(query)
      path = query.longest_entity_path.map do |entity|
        @workload[entity]
      end.reverse
      select = query.fields.map { |field| @workload.find_field field.value }
      eq = query.eq_fields.map do |condition|
        @workload.find_field condition.field.value
      end

      range = query.order_by.map { |field| @workload.find_field field }
      range << @workload.find_field(query.range_field.field.value) \
        unless query.range_field.nil?

      indexes_for_path path, select,
                       eq.group_by(&:parent), range.group_by(&:parent)
    end

    # Produce all possible indices for a given workload
    # @return [Array<Index>]
    def indexes_for_workload
      indexes = @workload.queries.map do |query|
        indexes_for_query(query).to_set.add query.materialize_view(@workload)
      end.inject(Set.new, &:+)

      # Combine the data of indices based on matching hash fields
      indexes.select do |index|
        index.order_fields.empty?
      end.group_by(&:hash_fields).each do |hash_fields, hash_indexes|
        extra_choices = hash_indexes.map do |index|
          [index.extra, index.path]
        end.uniq
        combos = 2.upto(extra_choices.count).map do |n|
          extra_choices.combination(n).to_a.uniq
        end.inject(Set.new, &:+)

        combos.map do |combo|
          extra = combo.map(&:first)
          paths = combo.map(&:last)

          # Check that the paths have a common prefix
          next unless paths.sort_by(&:length).reverse.inject do |a, b|
            a.prefixes.member?(b) && b
          end

          indexes.add Index.new hash_fields, [], extra.inject(Set.new, &:+),
                                paths.sort_by(&:length).last
        end
      end

      # Exclude things covered by the base indices
      indexes.reject do |index|
        index.path.length == 1 &&
        index.hash_fields.to_set == index.path.first.id_fields.to_set &&
        index.order_fields.empty?
      end
    end

    private

    # Produce all possible indices for a given path through the entity graph
    # which select the given fields and possibly allow equality/range filtering
    def indexes_for_path(path, select, eq, range)
      indexes = Set.new

      path.each_with_index do |_, i|
        path[i..-1].each_with_index do |_, j|
          j += i + 1
          indexes += indexes_for_step path[i..j], select, eq, range
        end
      end

      indexes
    end

    # Get all possible index fields which jump a path with a set of filters
    def index_choices(path, eq)
      eq_fields = path.map { |entity| eq[entity] }.compact.flatten

      # If we have no filtering on the first entity, add the ID fields
      eq_fields += path[0].id_fields if eq[path[0]].nil?

      eq_choices = 1.upto(eq_fields.count).map do |n|
        eq_fields.permutation(n).to_a
      end.inject([], &:+)

      eq_choices
    end

    # Get fields which should be included in an index for the given path
    def extra_choices(path, select, eq, range)
      last = path[-1]
      if select[0].parent == last
        [select[0].parent.id_fields, select]
      else
        filter_choices = (eq[last] || []) + (range[last] || [])
        choices = [last.id_fields]
        choices << filter_choices unless filter_choices.empty?
        choices
      end
    end

    # Get all possible indices which jump a given section in a query path
    def indexes_for_step(path, select, eq, range)
      index_choices = index_choices path, eq
      max_eq_fields = index_choices.map(&:length).max

      range_fields = path.map { |entity| range[entity] || [] }.reduce(&:+)
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

          indexes << Index.new(index, order, extra - (index + order), path)
        end

        # Partition into the ordering portion
        if index.length == max_eq_fields
          index.partitions.each do |index_prefix, order_prefix|
            indexes << Index.new(index_prefix, order_prefix + order,
                                 extra, path)
          end
        end

        indexes
      end.inject([], &:+).flatten
    end
  end
end
