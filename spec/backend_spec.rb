require 'nose/backend/cassandra'

module NoSE::Backend
  describe CassandraBackend do
    include_context 'entities'
    let(:index) do
      NoSE::Index.new [user['Username']],
                      [tweet['Timestamp'], tweet['TweetId']],
                      [tweet['Body']],
                      [user, tweet], 'TweetIndex'
    end
    let(:backend) { CassandraBackend.new workload, [index], [], {} }

    it 'can generate DDL for a simple index' do
      expect(backend.indexes_ddl).to match_array [
        'CREATE COLUMNFAMILY "TweetIndex" ("User_Username" text, ' \
        '"Tweet_Timestamp" int, "Tweet_TweetId" int, "Tweet_Body" text, ' \
        'PRIMARY KEY(("User_Username"), "Tweet_Timestamp", "Tweet_TweetId"));'
      ]
    end
  end

  describe BackendBase::SortQueryStep do
    include_context 'entities'

    it 'can sort a list of results' do
      results = [
        {'User_Username' => 'Bob'},
        {'User_Username' => 'Alice'}
      ]
      step = NoSE::Plans::SortPlanStep.new [user['Username']]

      BackendBase::SortQueryStep.process nil, nil, results, step, nil, nil

      expect(results).to eq [
        {'User_Username' => 'Alice'},
        {'User_Username' => 'Bob'}
      ]
    end
  end

  describe BackendBase::FilterQueryStep do
    include_context 'entities'

    it 'can filter results by an equality predicate' do
      results = [
        {'User_Username' => 'Alice'},
        {'User_Username' => 'Bob'}
      ]
      step = NoSE::Plans::FilterPlanStep.new [user['Username']], nil
      query = NoSE::Query.new 'SELECT * FROM User WHERE User.Username = "Bob"',
                              workload.model

      BackendBase::FilterQueryStep.process nil, query, results, step, nil, nil

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
      query = NoSE::Query.new 'SELECT * FROM User WHERE User.Username < "B" ' \
                              'AND User.City = "New York"', workload.model

      BackendBase::FilterQueryStep.process nil, query, results, step, nil, nil

      expect(results).to eq [
        {'User_Username' => 'Alice'}
      ]
    end
  end
end
