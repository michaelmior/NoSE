require 'nose/backend/file'

module NoSE
  module Backend
    describe FileBackend do
      include_context 'dummy_cost_model'
      include_context 'entities'

      let(:index_data) do
        {
          user.simple_index.key => [{
            'User_UserId'   => '18a9a155-c9c7-43b5-9ab0-5967c49f56e9',
            'User_Username' => 'Bob'
          }],

          tweet.simple_index.key => [{
            'Tweet_Timestamp' => Time.now,
            'Tweet_TweetId'   => 'e2dee9ee-5297-4f91-a3f7-9dd169008407',
            'Tweet_Body'      => 'This is a test'
          }],

          index.key => [{
            'User_Username'   => 'Bob',
            'Tweet_Timestamp' => Time.now,
            'User_UserId'     => '18a9a155-c9c7-43b5-9ab0-5967c49f56e9',
            'Tweet_TweetId'   => 'e2dee9ee-5297-4f91-a3f7-9dd169008407',
            'Tweet_Body'      => 'This is a test'
          }]
        }
      end

      let(:backend) do
        backend = FileBackend.new workload, [index], [], [], {}

        backend.instance_variable_set :@index_data, index_data

        backend
      end

      let(:query) do
        Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                        'WHERE User.Username = "Bob" ' \
                        'ORDER BY Tweet.Timestamp LIMIT 10', workload.model
      end

      it 'uses index descriptions for ddl' do
        expect(backend.indexes_ddl).to match_array [
          index.inspect
        ]
      end

      it 'can look up results based on a query plan' do
        # Materialize a view for the given query
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

      it 'can prepare a query' do
        planner = Plans::QueryPlanner.new workload.model, [index], cost_model
        plan = planner.min_plan(query)
        prepared = backend.prepare query, [plan]

        expect(prepared.steps).to have(1).item
        expect(prepared.steps.first).to be_a \
          FileBackend::IndexLookupStatementStep

        result = prepared.execute(
          'User_Username' => Condition.new(user['Username'], :'=', 'Bob')
        )

        expect(result).to eq index_data[index.key]
      end
    end
  end
end
