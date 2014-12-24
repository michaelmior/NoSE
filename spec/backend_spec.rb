require 'nose/backends/cassandra'

module NoSE
  describe CassandraBackend do
    include_context 'entities'
    let(:index) do
      Index.new [user['Username']],
                [tweet['Timestamp']],
                [tweet['Body']],
                [user, tweet], 'TweetIndex'
    end
    let(:backend) { CassandraBackend.new workload, [index], [], {} }

    it 'can generate DDL for a simple index' do
      expect(backend.indexes_ddl).to match_array [
        'CREATE COLUMNFAMILY "TweetIndex" ("User_Username" text, ' \
        '"Tweet_Timestamp" int, "Tweet_Body" text, "Tweet_TweetId" int, ' \
        'PRIMARY KEY(("User_Username"), "Tweet_Timestamp", "Tweet_TweetId"));'
      ]
    end
  end
end
