require 'nose/backend/mongo'

module NoSE
  module Backend
    describe MongoBackend do
      let(:config) do
        {
          name: 'mongo',
          uri: 'mongodb://localhost:27017/',
          database: 'nose'
        }
      end

      let(:plans) { Plans::ExecutionPlans.load 'ebay' }

      let(:backend) do
        MongoBackend.new plans.schema.model, plans.schema.indexes.values,
                         [], [], config
      end

      # Insert a new document for testing purposes
      # @return [BSON::ObjectId]
      def insert(index_key, values)
        index_key = plans.schema.indexes[index_key].to_id_graph.key
        client = backend.send :client
        client[index_key].drop
        client[index_key].insert_one(values).inserted_id
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

      it 'can query for inserted documents', mongo: true do
        id = insert('items_by_id', 'Title' => 'Foo')

        result = query('GetItem', 'items_ItemID' => id)
        expect(result).to have(1).item
        expect(result.first['items_Title']).to eq('Foo')
      end
    end
  end
end
