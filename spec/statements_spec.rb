module NoSE
  shared_examples 'a statement' do
    it 'tracks the range field' do
      expect(statement.range_field).to eq tweet['Timestamp']
    end

    it 'tracks fields used in equality predicates' do
      expect(statement.eq_fields).to match_array [user['City']]
    end

    it 'can report the longest entity path' do
      expect(statement.longest_entity_path).to match_array [tweet, user]
    end
  end

  describe Query do
    include_context 'entities'

    let(:query) do
      Query.new 'SELECT Tweet.TweetId FROM Tweet.User WHERE ' \
                'Tweet.Timestamp > ? AND User.City = ? ' \
                'ORDER BY Tweet.Timestamp LIMIT 5', workload.model
    end

    it_behaves_like 'a statement' do
      let(:statement) { query }
    end

    it 'can be converted back to query text' do
      expect(query.unparse).to eq query.text
    end

    it 'knows its limits' do
      expect(query.limit).to eq 5
    end

    it 'keeps a list of selected fields' do
      expect(query.select).to match_array [tweet['TweetId']]
    end

    it 'can select all fields' do
      stmt = Query.new 'SELECT Tweet.* FROM Tweet WHERE Tweet.Body = ?',
                       workload.model
      expect(stmt.select).to match_array tweet.fields.values
    end

    it 'compares equal regardless of constant values' do
      stmt1 = Query.new 'SELECT Tweet.* FROM Tweet WHERE Tweet.Retweets = 3',
                        workload.model
      stmt2 = Query.new 'SELECT Tweet.* FROM Tweet WHERE Tweet.Retweets = 2',
                        workload.model

      expect(stmt1).to eq stmt2
    end

    context 'when parsing literals' do
      it 'can find strings' do
        stmt = Query.new 'SELECT User.* FROM User WHERE User.City = "NY"',
                         workload.model
        expect(stmt.conditions['User_City'].value).to eq 'NY'
      end

      it 'can find integers' do
        stmt = Query.new 'SELECT Tweet.* FROM Tweet WHERE Tweet.Retweets = 3',
                         workload.model
        expect(stmt.conditions['Tweet_Retweets'].value).to eq 3
      end

      it 'fails if the value is the wrong type' do
        expect do
          Query.new 'SELECT Tweet.* FROM Tweet WHERE Tweet.Timestamp = 3',
                    workload.model
        end.to raise_error TypeError
      end
    end

    it 'can select additional hash fields' do
      query = Query.new 'SELECT User.** FROM User WHERE User.UserId = ?',
                        workload.model
      expect(query.select).to match_array [user['**']]
    end

    it 'fails if a field does not exist' do
      expect do
        Query.new 'SELECT User.Banana FROM User WHERE User.City = ?',
                  workload.model
      end.to raise_error FieldNotFound
    end

    it 'does not allow predicates on foreign keys' do
      expect do
        Query.new 'SELECT Tweet.* FROM Tweet WHERE Tweet.User = ?',
                  workload.model
      end.to raise_error InvalidStatementException
    end
  end

  describe Update do
    include_context 'entities'

    let(:update) do
      Update.new 'UPDATE Tweet FROM Tweet.User SET Body = "foo" WHERE ' \
                 'Tweet.Timestamp > ? AND User.City = ?', workload.model
    end

    it_behaves_like 'a statement' do
      let(:statement) { update }
    end

    it 'can parse field settings' do
      expect(update.settings).to match_array [
        FieldSetting.new(tweet['Body'], 'foo')
      ]
    end

    it 'does not produce a support query for unaffected indexes' do
      update = Update.new 'UPDATE User SET City = ? WHERE User.UserId = ?',
                          workload.model
      index = NoSE::Index.new [tweet['TweetId']], [], [tweet['Timestamp']],
                              QueryGraph::Graph.from_path(
                                [tweet.id_field]
                              ), workload.model
      expect(update.support_queries index).to be_empty
    end

    it 'can generate support queries' do
      update = Update.new 'UPDATE User SET City = ? WHERE User.UserId = ?',
                          workload.model
      index = NoSE::Index.new [tweet['Timestamp']],
                              [tweet['TweetId'], user['UserId']],
                              [user['City']],
                              QueryGraph::Graph.from_path(
                                [tweet.id_field, tweet['User']]
                              ), workload.model
      query = update.support_queries(index).first
      expect(query.text).to start_with \
        'SELECT Tweet.Timestamp, Tweet.TweetId ' \
        'FROM Tweet.User WHERE User.UserId = ?'
      expect(query.statement).to eq(update)
      expect(query.index).to eq(index)
    end

    it 'does not select fields with update predicates in support queries' do
      update = Update.new 'UPDATE User SET City = ? WHERE User.UserId = ?',
                          workload.model
      index = NoSE::Index.new [user['Username'], user['UserId']], [],
                              [user['City']], QueryGraph::Graph.from_path(
                                [user.id_field]
                              ), workload.model
      expect(update.support_queries(index).first.text).to start_with \
        'SELECT User.Username FROM User WHERE User.UserId = ?'
    end

    it 'fails if the FROM clause does not start with the updated entity' do
      expect do
        Update.new 'UPDATE User FROM Tweet.User SET City = ? ' \
                   'WHERE User.UserId = ?', workload.model
      end.to raise_error InvalidStatementException
    end
  end

  describe Insert do
    include_context 'entities'

    let(:insert) do
      Insert.new 'INSERT INTO Tweet SET Body = "Test", TweetId = "1" ' \
                 'AND CONNECT TO User("1"), Link("1")',
                 workload.model
    end

    it 'can be converted back to insert text' do
      expect(insert.unparse).to eq insert.text
    end

    it 'can parse field settings' do
      expect(insert.settings).to match_array [
        FieldSetting.new(tweet['Body'], 'Test'),
        FieldSetting.new(tweet['TweetId'], '1')
      ]
    end

    it 'can parse connections' do
      expect(insert.conditions.values).to match_array [
        Condition.new(tweet['User'], :'=', '1'),
        Condition.new(tweet['Link'], :'=', '1')
      ]
    end

    it 'knows which entity is being inserted' do
      expect(insert.entity).to eq(tweet)
    end

    it 'does not require a support query if only related IDs are used' do
      index = Index.new [user['UserId']], [tweet['TweetId']], [tweet['Body']],
                        QueryGraph::Graph.from_path([user['UserId'],
                                                     user['Tweets']])
      expect(insert.support_queries index).to be_empty
    end

    it 'uses a support query for connected entities' do
      index = Index.new [user['Username']], [user['UserId'], tweet['TweetId']],
                        [tweet['Body']], QueryGraph::Graph.from_path(
                          [user['UserId'], user['Tweets']]
                        )
      queries = insert.support_queries index
      expect(queries).to have(1).item
      expect(queries.first.text).to start_with \
        'SELECT User.Username FROM User WHERE User.UserId = ?'
    end
  end

  describe Delete do
    include_context 'entities'

    let(:delete) do
      Delete.new 'DELETE Tweet FROM Tweet.User WHERE ' \
                 'Tweet.Timestamp > ? AND User.City = ?', workload.model
    end

    it_behaves_like 'a statement' do
      let(:statement) { delete }
    end
  end

  describe Connection do
    include_context 'entities'

    it 'modifies an index if it crosses the path' do
      index = Index.new [user['UserId']], [tweet['TweetId']], [],
                        QueryGraph::Graph.from_path([user['UserId'],
                                                     user['Tweets']])
      connect = Connect.new 'CONNECT Tweet("A") TO User("B")', workload.model

      expect(connect.modifies_index? index).to be true
    end

    it 'modifies an index if crosses the path backwards' do
      index = Index.new [user['UserId']], [tweet['TweetId']], [],
                        QueryGraph::Graph.from_path([tweet['TweetId'],
                                                     tweet['User']])
      connect = Connect.new 'CONNECT Tweet("A") TO User("B")', workload.model

      expect(connect.modifies_index? index).to be true
    end

    it 'does not modify an index with a different path' do
      index = Index.new [user['UserId']], [tweet['TweetId']], [],
                        QueryGraph::Graph.from_path(
                          [user['UserId'], user['Favourite']]
                        )
      connect = Connect.new 'CONNECT Tweet("A") TO User("B")', workload.model

      expect(connect.modifies_index? index).to be false
    end

    it 'can generate support queries' do
      index = Index.new [user['UserId']], [tweet['TweetId']], [user['City']],
                        QueryGraph::Graph.from_path([tweet['TweetId'],
                                                     tweet['User']])
      connect = Connect.new 'CONNECT Tweet("A") TO User("B")', workload.model

      queries = connect.support_queries index
      expect(queries).to have(1).item
      expect(queries.first.text).to start_with \
        'SELECT User.City FROM User WHERE User.UserId = ?'
    end

    it 'does not require support queries if all fields are given' do
      index = Index.new [user['UserId']], [tweet['TweetId']], [],
                        QueryGraph::Graph.from_path([user['UserId'],
                                                     user['Favourite']])
      connect = Connect.new 'CONNECT Tweet("A") TO User("B")', workload.model

      expect(connect.support_queries(index)).to be_empty
    end

    it 'can generate support queries' do
      index = Index.new [user['UserId']], [tweet['TweetId']], [user['City']],
                        QueryGraph::Graph.from_path([user['UserId'],
                                                     user['Favourite']])
      disconnect = Disconnect.new 'DISCONNECT Tweet("A") FROM User("B")',
                                  workload.model

      expect(disconnect.support_queries(index)).to be_empty
    end
  end

  describe Connect do
    include_context 'entities'

    it 'can parse simple connect statements' do
      connect = Connect.new 'CONNECT Tweet("A") TO User("B")', workload.model

      expect(connect.source).to eq(tweet)
      expect(connect.source_pk).to eq('A')
      expect(connect.target).to eq(tweet['User'])
      expect(connect.target_pk).to eq('B')
    end

    it 'can parse parameterized connect statements' do
      connect = Connect.new 'CONNECT Tweet(?) TO User(?)', workload.model

      expect(connect.source).to eq(tweet)
      expect(connect.source_pk).to be_nil
      expect(connect.target).to eq(tweet['User'])
      expect(connect.target_pk).to be_nil
    end
  end

  describe Disconnect do
    include_context 'entities'

    it 'can parse simple disconnect statements' do
      connect = Disconnect.new 'DISCONNECT Tweet("A") FROM User("B")',
                               workload.model

      expect(connect.source).to eq(tweet)
      expect(connect.source_pk).to eq('A')
      expect(connect.target).to eq(tweet['User'])
      expect(connect.target_pk).to eq('B')
    end
  end

  describe KeyPath do
    include_context 'entities'

    context 'when constructing a KeyPath' do
      it 'fails if the first field is not a primary key' do
        expect do
          KeyPath.new [user['Tweets']]
        end.to raise_error InvalidKeyPathException
      end

      it 'fails if keys do not match' do
        expect do
          KeyPath.new [user.id_field, tweet['User']]
        end.to raise_error InvalidKeyPathException
      end
    end

    it 'can give the list of entities traversed' do
      key_path = KeyPath.new [user.id_field, user['Tweets']]
      expect(key_path.entities).to match_array [user, tweet]
    end

    it 'can give a key from a single index' do
      key_path = KeyPath.new [user.id_field, user['Tweets']]
      expect(key_path[1]).to eq(tweet.id_field)
    end

    it 'can give a new path from a range' do
      key_path = KeyPath.new [user.id_field,
                              user['Tweets'],
                              tweet['Link']]
      new_path = KeyPath.new [tweet.id_field, tweet['Link']]
      expect(key_path[1..2]).to eq(new_path)
    end

    it 'can reverse itself' do
      key_path = KeyPath.new [user.id_field, user['Tweets']]
      reverse_path = KeyPath.new [tweet.id_field, tweet['User']]
      expect(key_path.reverse).to eq(reverse_path)
    end

    it 'can order on multiple fields' do
      query = Query.new 'SELECT Tweet.TweetId FROM Tweet.User ' \
                        'WHERE User.UserId = ? ' \
                        'ORDER BY Tweet.Timestamp, Tweet.Retweets',
                        workload.model
      expect(query.order).to match_array [tweet['Timestamp'],
                                          tweet['Retweets']]
    end

    context 'when checking if something is included in the path' do
      let(:path) { KeyPath.new [user['UserId'], user['Tweets']] }

      it 'includes IDs of all entities on the path' do
        expect(path).to include tweet['TweetId']
      end

      it 'includes keys traversed by the path' do
        expect(path).to include user['Tweets']
      end
    end
  end
end
