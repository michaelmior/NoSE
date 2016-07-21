require 'nose/backend/cassandra'
require 'nose/backend/file'

module NoSE
  module Backend
    describe CassandraBackend do
      include_context 'dummy_cost_model'
      include_context 'entities'

      let(:backend) { CassandraBackend.new workload, [index], [], [], {} }

      it 'can generate DDL for a simple index' do
        expect(backend.indexes_ddl).to match_array [
          'CREATE COLUMNFAMILY "TweetIndex" ("User_Username" text, ' \
          '"Tweet_Timestamp" timestamp, "User_UserId" uuid, '\
          '"Tweet_TweetId" uuid, ' \
          '"Tweet_Body" text, PRIMARY KEY(("User_Username"), ' \
          '"Tweet_Timestamp", "User_UserId", "Tweet_TweetId"));'
        ]
      end

      it 'can lookup data for an index based on a plan' do
        # Materialize a view for the given query
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                                'WHERE User.Username = "Bob" ' \
                                'ORDER BY Tweet.Timestamp LIMIT 10',
                                workload.model
        index = query.materialize_view
        planner = Plans::QueryPlanner.new workload.model, [index], cost_model
        step = planner.min_plan(query).first

        # Validate the expected CQL query
        client = double('client')
        backend_query = 'SELECT User_Username, Tweet_Timestamp, Tweet_Body ' \
                        "FROM \"#{index.key}\" WHERE User_Username = ? " \
                        'ORDER BY Tweet_Timestamp LIMIT 10'
        expect(client).to receive(:prepare).with(backend_query) \
          .and_return(backend_query)

        # Define a simple array providing empty results
        results = []
        def results.last_page?
          true
        end
        expect(client).to receive(:execute) \
          .with(backend_query, 'Bob').and_return(results)

        step_class = CassandraBackend::IndexLookupStatementStep
        prepared = step_class.new client, query.all_fields, query.conditions,
                                  step, nil, step.parent
        prepared.process query.conditions, nil
      end

      it 'can insert into an index' do
        client = double('client')
        index = link.simple_index
        values = [{
          'Link_LinkId' => nil,
          'Link_URL' => 'http://www.example.com/'
        }]
        backend_insert = "INSERT INTO #{index.key} (Link_LinkId, Link_URL) " \
                         'VALUES (?, ?)'
        expect(client).to receive(:prepare).with(backend_insert) \
          .and_return(backend_insert)
        expect(client).to receive(:execute) \
          .with(backend_insert, kind_of(Cassandra::Uuid),
                'http://www.example.com/')

        step_class = CassandraBackend::InsertStatementStep
        prepared = step_class.new client, index, [link['LinkId'], link['URL']]
        prepared.process values
      end
    end

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

    describe BackendBase::SortStatementStep do
      include_context 'entities'

      it 'can sort a list of results' do
        results = [
          { 'User_Username' => 'Bob' },
          { 'User_Username' => 'Alice' }
        ]
        step = Plans::SortPlanStep.new [user['Username']]

        step_class = BackendBase::SortStatementStep
        prepared = step_class.new nil, [], {}, step, nil, nil
        results = prepared.process nil, results

        expect(results).to eq [
          { 'User_Username' => 'Alice' },
          { 'User_Username' => 'Bob' }
        ]
      end
    end

    describe BackendBase::FilterStatementStep do
      include_context 'entities'

      it 'can filter results by an equality predicate' do
        results = [
          { 'User_Username' => 'Alice' },
          { 'User_Username' => 'Bob' }
        ]
        step = Plans::FilterPlanStep.new [user['Username']], nil
        query = Statement.parse 'SELECT User.* FROM User ' \
                                'WHERE User.Username = "Bob"', workload.model

        step_class = BackendBase::FilterStatementStep
        prepared = step_class.new nil, [], {}, step, nil, nil
        results = prepared.process query.conditions, results

        expect(results).to eq [
          { 'User_Username' => 'Bob' }
        ]
      end

      it 'can filter results by a range predicate' do
        results = [
          { 'User_Username' => 'Alice' },
          { 'User_Username' => 'Bob' }
        ]
        step = Plans::FilterPlanStep.new [], [user['Username']]
        query = Statement.parse 'SELECT User.* FROM User WHERE ' \
                                'User.Username < "B" AND ' \
                                'User.City = "New York"', workload.model

        step_class = BackendBase::FilterStatementStep
        prepared = step_class.new nil, [], {}, step, nil, nil
        results = prepared.process query.conditions, results

        expect(results).to eq [
          { 'User_Username' => 'Alice' }
        ]
      end
    end

    describe BackendBase::FilterStatementStep do
      include_context 'entities'

      it 'can limit results' do
        results = [
          { 'User_Username' => 'Alice' },
          { 'User_Username' => 'Bob' }
        ]
        step = Plans::LimitPlanStep.new 1
        step_class = BackendBase::LimitStatementStep
        prepared = step_class.new nil, [], {}, step, nil, nil
        results = prepared.process({}, results)

        expect(results).to eq [
          { 'User_Username' => 'Alice' }
        ]
      end
    end
  end
end
