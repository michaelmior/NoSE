module NoSE
  shared_examples 'a statement' do
    it 'tracks the range field' do
      expect(statement.range_field).to eq tweet['Timestamp']
    end

    it 'tracks fields used in equality predicates' do
      expect(statement.eq_fields).to match_array [link['LinkId'], user['City']]
    end

    it 'can report the longest entity path' do
      expect(statement.longest_entity_path).to match_array [tweet, user, link]
    end
  end

  describe Query do
    include_context 'entities'

    let(:query) do
      Query.new 'SELECT TweetId FROM Tweet.User.Link WHERE ' \
                'Link.LinkId = ? AND Tweet.Timestamp > ? ' \
                'AND User.City = ? ORDER BY Tweet.Timestamp LIMIT 5',
                workload.model
    end

    it_behaves_like 'a statement' do
      let(:statement) { query }
    end

    it 'reports the entity being selected from' do
      expect(query.from).to eq tweet
    end

    it 'knows its limits' do
      expect(query.limit).to eq 5
    end

    it 'keeps a list of selected fields' do
      expect(query.select).to match_array [tweet['TweetId']]
    end

    it 'can select all fields' do
      stmt = Query.new 'SELECT * FROM Tweet WHERE Tweet.Body = ?',
                       workload.model
      expect(stmt.select).to match_array tweet.fields.values
    end

    it 'compares equal regardless of constant values' do
      stmt1 = Query.new 'SELECT * FROM Tweet WHERE Tweet.Timestamp = 3',
                        workload.model
      stmt2 = Query.new 'SELECT * FROM Tweet WHERE Tweet.Timestamp = 2',
                        workload.model

      expect(stmt1).to eq stmt2
    end

    context 'when parsing literals' do
      it 'can find strings' do
        stmt = Query.new 'SELECT * FROM User WHERE User.City = "NY"',
                         workload.model
        expect(stmt.conditions.first.value).to eq 'NY'
      end

      it 'can find integers' do
        stmt = Query.new 'SELECT * FROM Tweet WHERE Tweet.Timestamp = 3',
                         workload.model
        expect(stmt.conditions.first.value).to eq 3
      end

      it 'fails if the value is the wrong type' do
        expect do
          Query.new 'SELECT * FROM User WHERE User.City = 3', workload.model
        end.to raise_error TypeError
      end
    end

    it 'can select additional hash fields' do
      query = Query.new 'SELECT ** FROM User WHERE User.UserId = ?',
                        workload.model
      expect(query.select).to match_array [user['**']]
    end

    it 'fails if a field does not exist' do
      expect do
        Query.new 'SELECT Banana FROM User WHERE User.City = ?', workload.model
      end.to raise_error FieldNotFound
    end

    it 'does not allow predicates on foreign keys' do
      expect do
        Query.new 'SELECT * FROM Tweet WHERE Tweet.User = ?', workload.model
      end.to raise_error InvalidStatementException
    end
  end

  describe Update do
    include_context 'entities'

    let(:update) do
      Update.new 'UPDATE Tweet FROM Tweet.User.Link SET Body = "foo" WHERE ' \
                 'Link.LinkId = ? AND Tweet.Timestamp > ? ' \
                 'AND User.City = ?', workload.model
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
                              [tweet], workload.model
      expect(update.support_queries index).to be_empty
    end

    it 'can generate support queries' do
      update = Update.new 'UPDATE User SET City = ? WHERE User.UserId = ?',
                          workload.model
      index = NoSE::Index.new [tweet['Timestamp']],
                              [tweet['TweetId'], user['UserId']],
                              [user['City']], [tweet, user], workload.model
      query = update.support_queries(index).first
      expect(query.text).to eq \
        'SELECT Timestamp FROM Tweet.User WHERE User.UserId = ?'
      expect(query.statement).to eq(update)
      expect(query.index).to eq(index)
    end

    it 'does not select fields with update predicates in support queries' do
      update = Update.new 'UPDATE User SET City = ? WHERE User.UserId = ?',
                          workload.model
      index = NoSE::Index.new [user['Username'], user['UserId']], [],
                              [user['City']], [user], workload.model
      expect(update.support_queries(index).first.text).to eq \
        'SELECT Username FROM User WHERE User.UserId = ?'
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
      Insert.new 'INSERT INTO User SET Username = "Bob", City = "NY"',
                 workload.model
    end

    it 'can parse field settings' do
      expect(insert.settings).to match_array [
        FieldSetting.new(user['Username'], 'Bob'),
        FieldSetting.new(user['City'], 'NY')
      ]
    end

    it 'knows which entity is being inserted' do
      expect(insert.entity).to eq(user)
    end
  end

  describe Delete do
    include_context 'entities'

    let(:delete) do
      Delete.new 'DELETE Tweet FROM Tweet.User.Link WHERE ' \
                 'Link.LinkId = ? AND Tweet.Timestamp > ? ' \
                 'AND User.City = ?', workload.model
    end

    it_behaves_like 'a statement' do
      let(:statement) { delete }
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
  end
end
