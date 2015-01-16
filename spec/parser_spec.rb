module NoSE
  shared_examples 'a statement' do
    it 'tracks the range field' do
      expect(statement.range_field).to eq tweet['Timestamp']
    end

    it 'tracks fields used in equality predicates' do
      expect(statement.eq_fields).to match_array [tweet['Link'], user['City']]
    end

    it 'can report the longest entity path' do
      expect(statement.longest_entity_path).to match_array [tweet, user]
    end
  end

  describe Query do
    include_context 'entities'

    let(:query) do
      Query.new 'SELECT Body FROM Tweet.User WHERE ' \
                'Tweet.Link = ? AND Tweet.Timestamp > ? AND User.City = ? ' \
                'ORDER BY Tweet.Timestamp LIMIT 5', workload.model
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
      expect(query.select).to match_array [tweet['Body']]
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

  end

  describe Update do
    include_context 'entities'

    let(:update) do
      Update.new 'UPDATE Tweet.User SET Body = "foo" WHERE ' \
                 'Tweet.Link = ? AND Tweet.Timestamp > ? AND User.City = ?',
                 workload.model
    end

    it_behaves_like 'a statement' do
      let(:statement) { update }
    end

    it 'can parse field settings' do
      expect(update.settings).to match_array [
        FieldSetting.new(tweet['Body'], 'foo')
      ]
    end

    it 'can be converted to a query' do
      query = update.to_query
      expect(query.conditions).to eq update.conditions
      expect(query.from).to eq update.from
      expect(query.longest_entity_path).to eq update.longest_entity_path
      expect(query.select.to_set).to eq update.settings.map(&:field).to_set
    end
  end
end
