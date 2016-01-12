module NoSE
  module Plans
    # Superclass for steps using indices
    class IndexLookupPlanStep < PlanStep
      extend Forwardable

      attr_reader :index, :eq_filter, :range_filter, :limit, :order_by
      delegate hash: :index

      def initialize(index, state = nil, parent = nil)
        super()
        @index = index

        if state && state.query
          all_fields = state.query.all_fields
          @fields = (@index.hash_fields + @index.order_fields).to_set + \
                    (@index.extra.to_set & all_fields)
        else
          @fields = @index.all_fields
        end

        return if state.nil?
        @state = state.dup
        update_state parent
        @state.freeze
      end

      # :nocov:
      def to_color
        if @state.nil?
          "#{super} #{@index.to_color}"
        else
          "#{super} #{@index.to_color} * " \
            "#{@state.cardinality}/#{@state.hash_cardinality} "
        end
      end
      # :nocov:

      # Two index steps are equal if they use the same index
      def ==(other)
        other.instance_of?(self.class) && @index == other.index
      end
      alias_method :eql?, :==

      # Check if this step can be applied for the given index,
      # returning a possible application of the step
      def self.apply(parent, index, state)
        # Try reversing the path for this query
        state_path = state.path
        reversed = state_path.reverse
        if index.path.length > 1 &&
           reversed[0..index.path.length - 1] == index.path
          state_path = reversed
          reversed = true
        else
          reversed = false
        end

        # Check that this index is a valid jump in the path
        return nil unless state_path.start_with? index.path

        # We must move forward on paths at each lookup
        # XXX This disallows plans which look up additional attributes
        #     for entities other than the final one in the query path
        return nil if index.path.length == 1 && state_path.length > 1 &&
                      !parent.is_a?(RootPlanStep)
        return nil if index.identity? && state_path.length > 1

        return nil if invalid_parent_index? index, parent.parent_index

        # We need all hash fields to perform the lookup
        return nil unless index.hash_fields.all? do |field|
          (parent.fields + state.given_fields).include? field
        end

        # Get fields in the query relevant to this index
        # and check that they are provided for us here
        path_fields = state.fields_for_entities(index.path.entities).to_set
        path_fields -= parent.fields # exclude fields already fetched
        return nil unless path_fields.all?(&index.all_fields.method(:include?))

        # Use the reversed path for the remainder of the plan
        if reversed
          state = state.dup
          state.path = state_path
          state.freeze
        end

        return IndexLookupPlanStep.new(index, state, parent) \
          if last_fields?(index, state, state_path)

        nil
      end

      private

      # Check if this index can be used after the current parent
      def self.invalid_parent_index?(index, parent_index)
        return false if parent_index.nil?

        # If the last step gave an ID, we must use it
        # XXX This doesn't cover all cases
        parent_ids = parent_index.path.entities.last.id_fields.to_set
        has_ids = parent_ids.all?(&parent_index.extra.method(:include?))
        return true if has_ids && index.hash_fields.to_set != parent_ids

        # If we're looking up from a previous step, only allow lookup by ID
        return true unless (index.path.length == 1 &&
                           parent_index.path != index.path) ||
                           index.hash_fields == parent_ids
      end

      # Check that we have the required fields to move on with the next lookup
      def self.last_fields?(index, state, path)
        # Get the possible fields we need to select
        # This always includes the ID of the last and next entities
        # as well as the selected fields if we're at the end of the path
        last_choices = [index.path.entities.last.id_fields]
        next_key = path[index.path.length]
        last_choices << next_key.parent.id_fields unless next_key.nil?
        last_choices << state.fields if path == index.path

        last_choices.any? do |fields|
          fields.all?(&index.all_fields.method(:include?))
        end
      end

      # Modify the state to reflect the fields looked up by the index
      def update_state(parent)
        # Get the set of fields which can be filtered by the ordered keys
        order_prefix = (@state.eq - @index.hash_fields) & @index.order_fields
        order_prefix << @state.range unless @state.range.nil?
        order_prefix = order_prefix.zip(@index.order_fields)
        order_prefix = order_prefix.take_while { |x, y| x == y }.map(&:first)

        # Find fields which are filtered by the index
        @eq_filter = @state.eq & (@index.hash_fields + order_prefix).to_set
        @eq_filter += @index.hash_fields
        if order_prefix.include?(@state.range)
          @range_filter = @state.range
          @state.range = nil
        else
          @range_filter = nil
        end

        # Remove fields resolved by this index
        @state.fields -= @index.all_fields
        @state.eq -= @eq_filter

        # We can't resolve ordering if we're doing an ID lookup
        # since only one record exists per row (if it's the same entity)
        # We also need to have the fields used in order
        indexed_by_id = @index.hash_fields.include? @index.path.first
        order_prefix = @state.order_by.longest_common_prefix \
          @index.order_fields
        if indexed_by_id && order_prefix.map(&:parent).to_set ==
                            Set.new([index.path.entities.first])
          order_prefix = []
        else
          @state.order_by -= order_prefix
        end
        @order_by = order_prefix

        # Strip the path for this index, but if we haven't fetched all
        # fields, leave the last one so we can perform a separate ID lookup
        if @state.fields_for_entities(@index.path.entities,
                                      select: true).empty? &&
           @state.path == index.path
          @state.path = @state.path[index.path.length..-1]
        else
          @state.path = @state.path[index.path.length - 1..-1]
        end

        # Calculate the new cardinality assuming no limit
        # Hash cardinality starts at 1 or is the previous cardinality
        if parent.is_a?(RootPlanStep)
          @state.hash_cardinality = 1
        else
          @state.hash_cardinality = parent.state.cardinality
        end

        # Filter the total number of rows by filtering on non-hash fields
        cardinality = @index.per_hash_count * @state.hash_cardinality
        @state.cardinality = Cardinality.filter cardinality,
                                                @eq_filter -
                                                @index.hash_fields,
                                                @range_filter

        # Check if we can apply the limit from the query
        # This occurs either when we are on the first or last index lookup
        # and the ordering of the query has already been resolved
        order_resolved = @state.order_by.empty? && @state.path.length == 1
        if (@state.answered?(check_limit: false) ||
            parent.is_a?(RootPlanStep) && order_resolved) &&
           !@state.query.limit.nil?
          # XXX Assume that everything is limited by the limit value
          #     which should be fine if the limit is small enough
          @limit = @state.query.limit
          if parent.is_a?(RootPlanStep)
            @state.cardinality = [@limit, @state.cardinality].min
            @state.hash_cardinality = 1
          else
            @limit = @state.cardinality = @state.query.limit

            # If this is a final lookup by ID, go with the limit
            if index.path.length == 1 && indexed_by_id
              @state.hash_cardinality = @limit
            else
              @state.hash_cardinality = parent.state.cardinality
            end
          end
        end
      end
    end
  end
end
