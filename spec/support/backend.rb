module NoSE
  RSpec.shared_examples 'backend processing' do |tag|
    let(:plans) { Plans::ExecutionPlans.load 'ebay' }

    # Insert a new entity for testing purposes
    def direct_insert(index_key, values)
      backend.indexes_ddl(true, true, true).to_a

      index = plans.schema.indexes[index_key]
      index = index.to_id_graph if backend.by_id_graph
      inserted_ids = backend.index_insert_chunk index, [values]
      inserted_ids.first
    end

    # Get a record from a particular index
    # @return [Hash]
    def direct_query(index_key)
      index = plans.schema.indexes[index_key]
      index = index.to_id_graph if backend.by_id_graph

      backend.index_sample(index, 1).first
    end

    # Execute an insert statement against the backend
    # @return [void]
    def insert(group, values)
      modify group, values, {}
    end

    # Execute an update statement against the backend
    # @return [void]
    def update(group, settings, conditions)
      modify group, settings, conditions
    end

    # Execute a modification statement with
    # the given settings and conditions
    # @return [void]
    def modify(group, settings, conditions)
      backend.indexes_ddl(true, true, true).to_a

      update_plans = plans.groups[group]

      update_plans.each do |plan|
        # Decide which fields should be set
        plan_settings = settings.map do |field_id, value|
          field = plan.index.all_fields.find { |f| f.id == field_id }
          FieldSetting.new field, value
        end

        # Generate any missing IDs
        (plan.index.hash_fields + plan.index.order_fields).each do |field|
          setting = plan_settings.find { |s| s.field == field }
          next unless setting.nil?

          plan_settings << FieldSetting.new(field, backend.generate_id) \
            if field.is_a? Fields::IDField
        end

        # Build the list of conditions
        plan_conditions = Hash[conditions.map do |field_id, value|
          field = plan.index.all_fields.find { |f| f.id == field_id }
          [field_id, Condition.new(field, :'=', value)]
        end]

        prepared = backend.prepare_update nil, [plan]
        prepared.each { |p| p.execute plan_settings, plan_conditions }
      end
    end

    # Execute a query against the backend and return the results
    # @return [Hash]
    def query(group, values)
      plan = plans.groups[group].first
      prepared = backend.prepare_query nil, plan.select_fields, plan.params,
                                       [plan.steps]

      prepared.execute Hash[values.map do |k, v|
        condition = plan.params[k]
        condition.instance_variable_set :@value, v
        [k, condition]
      end]
    end

    it 'can query for inserted entities', tag do
      id = direct_insert 'items_by_id', 'items_Title' => 'Foo',
                                        'items_Desc'  => 'A thing'
      id = id.first if id.is_a? Array

      result = query 'GetItem', 'items_ItemID' => id
      expect(result).to have(1).item
      expect(result.first['items_Title']).to eq('Foo')
    end

    it 'can insert new entities', tag do
      insert 'AddItem', 'items_Title' => 'Foo', 'items_Desc' => 'A thing'

      result = direct_query 'items_by_id'
      expect(result).to include 'items_Title' => 'Foo'
    end

    it 'can update entities', tag do
      id = direct_insert 'items_by_id', 'items_Title' => 'Foo',
                                        'items_Desc'  => 'A thing'
      id = id.first if id.is_a? Array

      update 'UpdateItemTitle',
             { 'items_Title' => 'Bar' },
             'items_ItemID' => id

      result = direct_query 'items_by_id'
      expect(result).to include 'items_Title' => 'Bar'
    end
  end
end
