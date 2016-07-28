module NoSE
  # A representation of an update in the workload
  class Update < Statement
    include StatementConditions
    include StatementSettings
    include StatementSupportQuery

    def initialize(params, text, group: nil, label: nil)
      super params, text, group: group, label: label

      populate_conditions params
      @settings = params[:settings]
    end

    # Build a new update from a provided parse tree
    # @return [Update]
    def self.parse(tree, params, text, group: nil, label: nil)
      conditions_from_tree tree, params
      settings_from_tree tree, params

      Update.new params, text, group: group, label: label
    end

    # Produce the SQL text corresponding to this update
    # @return [String]
    def unparse
      update = "UPDATE #{entity.name} "
      update += "FROM #{from_path @key_path} "
      update += settings_clause
      update += where_clause

      update
    end

    def ==(other)
      other.is_a?(Update) &&
        @graph == other.graph &&
        entity == other.entity &&
        @settings == other.settings &&
        @conditions == other.conditions
    end
    alias eql? ==

    def hash
      @hash ||= [@graph, entity, @settings, @conditions].hash
    end

    # Specifies that updates require insertion
    def requires_insert?(_index)
      true
    end

    # Specifies that updates require deletion
    def requires_delete?(index)
      !(settings.map(&:field).to_set &
        (index.hash_fields + index.order_fields.to_set)).empty?
    end

    # Get the support queries for updating an index
    # @return [Array<SupportQuery>]
    def support_queries(index)
      # Get the updated fields and check if an update is necessary
      set_fields = settings.map(&:field).to_set

      # We only need to fetch all the fields if we're updating a key
      updated_key = !(set_fields &
                      (index.hash_fields + index.order_fields)).empty?

      updated_fields = set_fields & index.all_fields
      [support_query_for_fields(index, updated_fields, updated_key)].compact
    end

    # The condition fields are provided with the update
    # Note that we don't include the settings here because we
    # care about the previously existing values in the database
    def given_fields
      @conditions.each_value.map(&:field)
    end
  end
end
