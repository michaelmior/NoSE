require 'nose/backend/file'

module NoSE
  module Backend
    describe FileBackend do
      include_context 'dummy_cost_model'
      include_context 'entities'

      let(:backend) { FileBackend.new workload, [index], [], [], {} }

      it 'uses index descriptions for ddl' do
        expect(backend.indexes_ddl).to match_array [
          index.inspect
        ]
      end

      it 'can look up results based on a query plan' do
        # Materialize a view for the given query
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                                'WHERE User.Username = "Bob" ' \
                                'ORDER BY Tweet.Timestamp LIMIT 10',
                                workload.model
        index = query.materialize_view
        planner = Plans::QueryPlanner.new workload.model, [index], cost_model

        # Execute the planned query
        step = planner.min_plan(query).first
        index_data = { index.key => [{ 'User_Username' => 'Bob' }] }
        step_class = FileBackend::IndexLookupStatementStep
        prepared = step_class.new index_data, query.all_fields,
                                  query.conditions, step, nil, step.parent
        results = prepared.process query.conditions, nil

        # Verify we get the result we started with
        expect(results).to eq index_data[index.key]
      end

      it 'can insert into an index' do
        index = link.simple_index
        values = [{
          'Link_LinkId' => nil,
          'Link_URL' => 'http://www.example.com/'
        }]

        index_data = { index.key => [] }
        step_class = FileBackend::InsertStatementStep
        prepared = step_class.new index_data, index,
                                  [link['LinkId'], link['URL']]
        prepared.process values

        # Validate the inserted data
        data = index_data[index.key]
        expect(data).to have(1).item
        expect(data[0]).to have_key 'Link_LinkId'
        expect(data[0]['Link_URL']).to eq values[0]['Link_URL']
      end
    end
  end
end
