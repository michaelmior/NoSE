module NoSE
  RSpec.shared_examples 'backend processing' do |tag|
    let(:plans) { Plans::ExecutionPlans.load 'ebay' }

    # Insert a new document for testing purposes
    def insert(index_key, values)
      backend.indexes_ddl(true, true, true).to_a

      index = plans.schema.indexes[index_key]
      index = index.to_id_graph if backend.by_id_graph
      inserted_ids = backend.index_insert_chunk index, [values]
      inserted_ids.first
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

    it 'can query for inserted documents', tag do
      id = insert('items_by_id', 'items_Title' => 'Foo')
      id = id.first if id.is_a? Array

      result = query('GetItem', 'items_ItemID' => id)
      expect(result).to have(1).item
      expect(result.first['items_Title']).to eq('Foo')
    end
  end
end
