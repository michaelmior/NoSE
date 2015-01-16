require 'nose/backends/cassandra'

module NoSE
  describe Backends::CassandraBackend do
    include_context 'entities'
    let(:index) do
      Index.new [user['Username']],
                [tweet['Timestamp']],
                [tweet['Body']],
                [user, tweet], 'TweetIndex'
    end
    let(:backend) { Backends::CassandraBackend.new workload, [index], [], {} }

    it 'can generate DDL for a simple index' do
      expect(backend.indexes_ddl).to match_array [
        'CREATE COLUMNFAMILY "TweetIndex" ("User_Username" text, ' \
        '"Tweet_Timestamp" int, "Tweet_Body" text, "Tweet_TweetId" int, ' \
        'PRIMARY KEY(("User_Username"), "Tweet_Timestamp", "Tweet_TweetId"));'
      ]
    end
  end

  describe Backend::SortQueryStep do
    include_context 'entities'

    it 'can sort a list of results' do
      results = [
        {'User_Username' => 'Bob'},
        {'User_Username' => 'Alice'}
      ]
      step = SortPlanStep.new [user['Username']]

      Backend::SortQueryStep.process nil, nil, results, step, nil, nil

      expect(results).to eq [
        {'User_Username' => 'Alice'},
        {'User_Username' => 'Bob'}
      ]
    end
  end

  describe Backend::FilterQueryStep do
    include_context 'entities'

    it 'can filter results by an equality predicate' do
      results = [
        {'User_Username' => 'Alice'},
        {'User_Username' => 'Bob'}
      ]
      step = FilterPlanStep.new [user['Username']], nil
      query = Query.new 'SELECT * FROM User WHERE User.Username = "Bob"',
                        workload.model

      Backend::FilterQueryStep.process nil, query, results, step, nil, nil

      expect(results).to eq [
        {'User_Username' => 'Bob'}
      ]
    end

    it 'can filter results by a range predicate' do
      results = [
        {'User_Username' => 'Alice'},
        {'User_Username' => 'Bob'}
      ]
      step = FilterPlanStep.new [], [user['Username']]
      query = Query.new 'SELECT * FROM User WHERE User.Username < "B" ' \
                        'AND User.City = "New York"', workload.model

      Backend::FilterQueryStep.process nil, query, results, step, nil, nil

      expect(results).to eq [
        {'User_Username' => 'Alice'}
      ]
    end
  end
end
