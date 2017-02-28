module NoSE
  shared_examples 'a statement' do
    it 'tracks the range field' do
      expect(statement.range_field).to eq tweet['Timestamp']
    end

    it 'tracks fields used in equality predicates' do
      expect(statement.eq_fields).to match_array [user['City']]
    end

    it 'finds entities along the path in the from clause' do
      expect(statement.key_path.entities).to match_array [tweet, user]
    end
  end

  describe Query do
    include_context 'entities'

    let(:query) do
      Statement.parse 'SELECT Tweet.TweetId FROM Tweet.User WHERE ' \
                      'Tweet.Timestamp > ? AND User.City = ? ' \
                      'ORDER BY Tweet.Timestamp LIMIT 5', workload.model
    end

    it_behaves_like 'a statement' do
      let(:statement) { query }
    end

    it 'can be converted back to query text' do
      expect(query.unparse).to start_with query.text
    end

    it 'knows its limits' do
      expect(query.limit).to eq 5
    end

    it 'keeps a list of selected fields' do
      expect(query.select).to match_array [tweet['TweetId']]
    end

    it 'can select all fields' do
      stmt = Statement.parse 'SELECT Tweet.* FROM Tweet WHERE Tweet.Body = ?',
                             workload.model
      expect(stmt.select).to match_array tweet.fields.values
    end

    it 'compares equal regardless of constant values' do
      stmt1 = Statement.parse 'SELECT Tweet.* FROM Tweet ' \
                              'WHERE Tweet.Retweets = 3', workload.model
      stmt2 = Statement.parse 'SELECT Tweet.* FROM Tweet ' \
                              'WHERE Tweet.Retweets = 2', workload.model

      expect(stmt1).to eq stmt2
    end

    context 'when parsing literals' do
      it 'can find strings' do
        stmt = Statement.parse 'SELECT User.* FROM User ' \
                               'WHERE User.City = "NY"', workload.model
        expect(stmt.conditions['User_City'].value).to eq 'NY'
      end

      it 'can find integers' do
        stmt = Statement.parse 'SELECT Tweet.* FROM Tweet ' \
                               'WHERE Tweet.Retweets = 3', workload.model
        expect(stmt.conditions['Tweet_Retweets'].value).to eq 3
      end

      it 'fails if the value is the wrong type' do
        expect do
          Statement.parse 'SELECT Tweet.* FROM Tweet ' \
                          'WHERE Tweet.Timestamp = 3', workload.model
        end.to raise_error TypeError
      end
    end

    it 'can select additional hash fields' do
      query = Statement.parse 'SELECT User.** FROM User WHERE User.UserId = ?',
                              workload.model
      expect(query.select).to match_array [user['**']]
    end

    it 'fails if a field does not exist' do
      expect do
        Statement.parse 'SELECT User.Banana FROM User WHERE User.City = ?',
                        workload.model
      end.to raise_error FieldNotFound
    end

    it 'does not allow predicates on foreign keys' do
      expect do
        Statement.parse 'SELECT Tweet.* FROM Tweet WHERE Tweet.User = ?',
                        workload.model
      end.to raise_error InvalidStatementException
    end

    it 'can have branching in the fields being selected' do
      query = Statement.parse 'SELECT Tweet.Link.URL, Tweet.Body FROM Tweet ' \
                              'WHERE Tweet.TweetId= ?', workload.model
      graph = QueryGraph::Graph.from_path(
        [tweet.id_field, tweet['Link']]
      )

      expect(query.graph).to eq graph
    end
  end

  describe Update do
    include_context 'entities'

    let(:update) do
      Statement.parse 'UPDATE Tweet FROM Tweet.User SET Body = "foo" WHERE ' \
                      'Tweet.Timestamp > ? AND User.City = ?', workload.model
    end

    it_behaves_like 'a statement' do
      let(:statement) { update }
    end

    it 'can be converted back to update text' do
      expect(update.unparse).to eq update.text
    end

    it 'can parse field settings' do
      expect(update.settings).to match_array [
        FieldSetting.new(tweet['Body'], 'foo')
      ]
    end

    it 'does not produce a support query for unaffected indexes' do
      update = Statement.parse 'UPDATE User SET City = ? ' \
                               'WHERE User.UserId = ?', workload.model
      index = Index.new [tweet['TweetId']], [], [tweet['Timestamp']],
                        QueryGraph::Graph.from_path(
                          [tweet.id_field]
                        )
      expect(update.support_queries index).to be_empty
    end

    it 'can generate support queries' do
      update = Statement.parse 'UPDATE User SET City = ? WHERE ' \
                               'User.UserId = ?', workload.model
      index = Index.new [tweet['Timestamp']],
                        [tweet['TweetId'], user['UserId']],
                        [user['City']],
                        QueryGraph::Graph.from_path(
                          [tweet.id_field, tweet['User']]
                        )
      query = update.support_queries(index).first
      expect(query.unparse).to start_with \
        'SELECT Tweet.Timestamp, Tweet.TweetId ' \
        'FROM Tweet.User WHERE User.UserId = ?'
      expect(query.statement).to eq(update)
      expect(query.index).to eq(index)
    end

    it 'does not select fields with update predicates in support queries' do
      update = Statement.parse 'UPDATE User SET City = ? WHERE ' \
                               'User.UserId = ?', workload.model
      index = Index.new [user['Username'], user['UserId']], [],
                        [user['City']], QueryGraph::Graph.from_path(
                          [user.id_field]
                        )
      expect(update.support_queries(index).first.unparse).to start_with \
        'SELECT User.Username FROM User WHERE User.UserId = ?'
    end

    it 'fails if the FROM clause does not start with the updated entity' do
      expect do
        Statement.parse 'UPDATE User FROM Tweet.User SET City = ? ' \
                        'WHERE User.UserId = ?', workload.model
      end.to raise_error InvalidStatementException
    end
  end

  describe Insert do
    include_context 'entities'

    let(:insert) do
      Statement.parse 'INSERT INTO Tweet SET Body = "Test", TweetId = "1" ' \
                      'AND CONNECT TO User("1"), Link("1")', workload.model
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
      expect(queries.first.unparse).to start_with \
        'SELECT User.Username FROM User WHERE User.UserId = 1'
    end
  end

  describe Delete do
    include_context 'entities'

    let(:delete) do
      Statement.parse 'DELETE Tweet FROM Tweet.User WHERE ' \
                      'Tweet.Timestamp > ? AND User.City = ?', workload.model
    end

    it_behaves_like 'a statement' do
      let(:statement) { delete }
    end

    it 'can be converted back to delete text' do
      expect(delete.unparse).to eq delete.text
    end

    it 'can generate support queries' do
      delete = Statement.parse 'DELETE Tweet FROM Tweet.User WHERE ' \
                               'Tweet.Timestamp = ? AND User.City = ?',
                               workload.model
      index = Index.new [user['UserId']], [tweet['TweetId']], [tweet['Body']],
                        QueryGraph::Graph.from_path([tweet['TweetId'],
                                                     tweet['User']])
      queries = delete.support_queries index
      expect(queries).to have(2).items
      expect(queries.first.unparse).to start_with \
        'SELECT Tweet.TweetId FROM Tweet.User WHERE Tweet.Timestamp = ? ' \
        'AND User.City = ?'
      expect(queries.last.unparse).to start_with \
        'SELECT User.UserId FROM Tweet.User WHERE Tweet.TweetId = ?'
    end
  end

  describe Connection do
    include_context 'entities'

    it 'modifies an index if it crosses the path' do
      index = Index.new [user['UserId']], [tweet['TweetId']], [],
                        QueryGraph::Graph.from_path([user['UserId'],
                                                     user['Tweets']])
      connect = Statement.parse 'CONNECT Tweet("A") TO User("B")',
                                workload.model

      expect(connect.modifies_index? index).to be true
    end

    it 'modifies an index if crosses the path backwards' do
      index = Index.new [user['UserId']], [tweet['TweetId']], [],
                        QueryGraph::Graph.from_path([tweet['TweetId'],
                                                     tweet['User']])
      connect = Statement.parse 'CONNECT Tweet("A") TO User("B")',
                                workload.model

      expect(connect.modifies_index? index).to be true
    end

    it 'does not modify an index with a different path' do
      index = Index.new [user['UserId']], [tweet['TweetId']], [],
                        QueryGraph::Graph.from_path(
                          [user['UserId'], user['Favourite']]
                        )
      connect = Statement.parse 'CONNECT Tweet("A") TO User("B")',
                                workload.model

      expect(connect.modifies_index? index).to be false
    end

    it 'can generate support queries' do
      index = Index.new [user['UserId']], [tweet['TweetId']], [user['City']],
                        QueryGraph::Graph.from_path([tweet['TweetId'],
                                                     tweet['User']])
      connect = Statement.parse 'CONNECT Tweet("A") TO User("B")',
                                workload.model

      queries = connect.support_queries index
      expect(queries).to have(1).item
      expect(queries.first.unparse).to start_with \
        'SELECT User.City FROM User WHERE User.UserId = B'
    end

    it 'does not require support queries if all fields are given' do
      index = Index.new [user['UserId']], [tweet['TweetId']], [],
                        QueryGraph::Graph.from_path([user['UserId'],
                                                     user['Favourite']])
      connect = Statement.parse 'CONNECT Tweet("A") TO User("B")',
                                workload.model

      expect(connect.support_queries(index)).to be_empty
    end

    it 'can generate support queries' do
      index = Index.new [user['UserId']], [tweet['TweetId']], [user['City']],
                        QueryGraph::Graph.from_path([user['UserId'],
                                                     user['Favourite']])
      disconnect = Statement.parse 'DISCONNECT Tweet("A") FROM User("B")',
                                   workload.model

      expect(disconnect.support_queries(index)).to be_empty
    end
  end

  describe Connect do
    include_context 'entities'

    it 'can be converted back to connection text' do
      connect = Statement.parse 'CONNECT Tweet("A") TO User("B")',
                                workload.model
      expect(connect.unparse).to eq connect.text
    end

    it 'can parse simple connect statements' do
      connect = Statement.parse 'CONNECT Tweet("A") TO User("B")',
                                workload.model

      expect(connect.source).to eq(tweet)
      expect(connect.source_pk).to eq('A')
      expect(connect.target).to eq(tweet['User'])
      expect(connect.target_pk).to eq('B')
    end

    it 'can parse parameterized connect statements' do
      connect = Statement.parse 'CONNECT Tweet(?) TO User(?)', workload.model

      expect(connect.source).to eq(tweet)
      expect(connect.source_pk).to be_nil
      expect(connect.target).to eq(tweet['User'])
      expect(connect.target_pk).to be_nil
    end
  end

  describe Disconnect do
    include_context 'entities'

    it 'can be converted back to disconnection text' do
      disconnect = Statement.parse 'DISCONNECT Tweet("A") FROM User("B")',
                                   workload.model
      expect(disconnect.unparse).to eq disconnect.text
    end

    it 'can parse simple disconnect statements' do
      connect = Statement.parse 'DISCONNECT Tweet("A") FROM User("B")',
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
      query = Statement.parse 'SELECT Tweet.TweetId FROM Tweet.User ' \
                              'WHERE User.UserId = ? ' \
                              'ORDER BY Tweet.Timestamp, Tweet.Retweets',
                              workload.model
      expect(query.order).to match_array [tweet['Timestamp'],
                                          tweet['Retweets']]
    end

    it 'does not allow ordering by ID' do
      query = 'SELECT Tweet.TweetId FROM Tweet.User ' \
              'WHERE User.UserId = ? ORDER BY Tweet.TweetId'

      expect do
        Statement.parse query, workload.model
      end.to raise_error InvalidStatementException
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

    context 'when producing the path for a field' do
      it 'names fields on the first entity' do
        key_path = KeyPath.new [user.id_field]
        expect(key_path.path_for_field(user['City'])).to match_array ['City']
      end

      it 'names intermediate foreign keys' do
        key_path = KeyPath.new [user.id_field, user['Tweets'], tweet['Link']]
        expect(key_path.path_for_field(link['URL'])).to match_array \
          %w(Tweets Link URL)
      end

      it 'can produce partial paths' do
        key_path = KeyPath.new [user.id_field, user['Tweets'], tweet['Link']]
        expect(key_path.path_for_field(tweet['Body'])).to match_array \
          %w(Tweets Body)
      end
    end
  end
end
