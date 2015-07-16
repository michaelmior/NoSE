require 'nose/backend/cassandra'

module NoSE::Backend
  describe CassandraBackend do
    include_context 'dummy_cost_model'
    include_context 'entities'

    let(:index) do
      NoSE::Index.new [user['Username']],
                      [tweet['Timestamp'], user['UserId'], tweet['TweetId']],
                      [tweet['Body']],
                      [user.id_fields.first, user['Tweets']], 'TweetIndex'
    end
    let(:backend) { CassandraBackend.new workload, [index], [], [], {} }

    it 'can generate DDL for a simple index' do
      expect(backend.indexes_ddl).to match_array [
        'CREATE COLUMNFAMILY "TweetIndex" ("User_Username" text, ' \
        '"Tweet_Timestamp" timestamp, "User_UserId" timeuuid, '\
        '"Tweet_TweetId" timeuuid, ' \
        '"Tweet_Body" text, PRIMARY KEY(("User_Username"), ' \
        '"Tweet_Timestamp", "User_UserId", "Tweet_TweetId"));'
      ]
    end

    it 'can lookup data for an index based on a plan' do
      # Materialize a view for the given query
      query = NoSE::Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                                    'WHERE User.Username = "Bob" ' \
                                    'ORDER BY Tweet.Timestamp LIMIT 10', workload.model
      index = query.materialize_view
      planner = NoSE::Plans::QueryPlanner.new workload.model, [index],
                                              cost_model
      step = planner.min_plan(query).first

      # Validate the expected CQL query
      client = double('client')
      backend_query = "SELECT User_Username, Tweet_Timestamp, Tweet_Body " \
                      "FROM \"#{index.key}\" WHERE User_Username = ? " \
                      "ORDER BY Tweet_Timestamp LIMIT 10"
      expect(client).to receive(:prepare) { backend_query } \
        .and_return(backend_query)
      expect(client).to receive(:execute) { backend_query }.and_return([])

      CassandraBackend::IndexLookupStatementStep.process client, query, nil,
                                                         step, step.parent, nil
    end

  end

  describe BackendBase::SortStatementStep do
    include_context 'entities'

    it 'can sort a list of results' do
      results = [
        {'User_Username' => 'Bob'},
        {'User_Username' => 'Alice'}
      ]
      step = NoSE::Plans::SortPlanStep.new [user['Username']]

      BackendBase::SortStatementStep.process nil, nil, results, step, nil, nil

      expect(results).to eq [
        {'User_Username' => 'Alice'},
        {'User_Username' => 'Bob'}
      ]
    end
  end

  describe BackendBase::FilterStatementStep do
    include_context 'entities'

    it 'can filter results by an equality predicate' do
      results = [
        {'User_Username' => 'Alice'},
        {'User_Username' => 'Bob'}
      ]
      step = NoSE::Plans::FilterPlanStep.new [user['Username']], nil
      query = NoSE::Query.new 'SELECT User.* FROM User ' \
                              'WHERE User.Username = "Bob"', workload.model

      BackendBase::FilterStatementStep.process nil, query, results, step,
                                               nil, nil

      expect(results).to eq [
        {'User_Username' => 'Bob'}
      ]
    end

    it 'can filter results by a range predicate' do
      results = [
        {'User_Username' => 'Alice'},
        {'User_Username' => 'Bob'}
      ]
      step = NoSE::Plans::FilterPlanStep.new [], [user['Username']]
      query = NoSE::Query.new 'SELECT User.* FROM User WHERE ' \
                              'User.Username < "B" AND User.City = "New York"',
                              workload.model

      BackendBase::FilterStatementStep.process nil, query, results, step,
                                               nil, nil

      expect(results).to eq [
        {'User_Username' => 'Alice'}
      ]
    end
  end
end
