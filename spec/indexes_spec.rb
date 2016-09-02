module NoSE
  describe Index do
    include_context 'entities'

    let(:equality_query) do
      Statement.parse 'SELECT Tweet.Body FROM Tweet WHERE Tweet.TweetId = ?',
                      workload.model
    end
    let(:combo_query) do
      Statement.parse 'SELECT Tweet.Body FROM Tweet ' \
                      'WHERE Tweet.Timestamp > ? ' \
                      'AND Tweet.TweetId = ?', workload.model
    end
    let(:order_query) do
      Statement.parse 'SELECT Tweet.Body FROM Tweet WHERE Tweet.TweetId = ? ' \
                      'ORDER BY Tweet.Timestamp', workload.model
    end

    before(:each) do
      workload.add_statement equality_query
      workload.add_statement combo_query
      workload.add_statement order_query
    end

    it 'can return fields by field ID' do
      expect(index['Tweet_Body']).to eq(tweet['Body'])
    end

    it 'contains fields' do
      index = Index.new [tweet['TweetId']], [], [tweet['Body']],
                        QueryGraph::Graph.from_path([tweet.id_field])
      expect(index.contains_field? tweet['TweetId']).to be true
    end

    it 'can store additional fields' do
      index = Index.new [tweet['TweetId']], [], [tweet['Body']],
                        QueryGraph::Graph.from_path([tweet.id_field])
      expect(index.contains_field? tweet['Body']).to be true
    end

    it 'can calculate its size' do
      index = Index.new [tweet['TweetId']], [], [tweet['Body']],
                        QueryGraph::Graph.from_path([tweet.id_field])
      entry_size = tweet['TweetId'].size + tweet['Body'].size
      expect(index.entry_size).to eq(entry_size)
      expect(index.size).to eq(entry_size * tweet.count)
    end

    context 'when materializing views' do
      it 'supports equality predicates' do
        index = equality_query.materialize_view
        expect(index.hash_fields).to eq([tweet['TweetId']].to_set)
      end

      it 'support range queries' do
        index = combo_query.materialize_view
        expect(index.order_fields).to eq([tweet['Timestamp']])
      end

      it 'supports multiple predicates' do
        index = combo_query.materialize_view
        expect(index.hash_fields).to eq([tweet['TweetId']].to_set)
        expect(index.order_fields).to eq([tweet['Timestamp']])
      end

      it 'supports order by' do
        index = order_query.materialize_view
        expect(index.order_fields).to eq([tweet['Timestamp']])
      end

      it 'keeps a static key' do
        index = combo_query.materialize_view
        expect(index.key).to eq 'i1632091596'
      end

      it 'includes only one entity in the hash fields' do
        query = Statement.parse 'SELECT Tweet.TweetId FROM Tweet.User ' \
                                'WHERE Tweet.Timestamp = ? AND User.City = ?',
                                workload.model
        index = query.materialize_view
        expect(index.hash_fields.map(&:parent).uniq).to have(1).item
      end
    end

    it 'can tell if it maps identities for a field' do
      index = Index.new [tweet['TweetId']], [], [tweet['Body']],
                        QueryGraph::Graph.from_path([tweet.id_field])
      expect(index.identity?).to be true
    end

    it 'can be created to map entity fields by id' do
      index = tweet.simple_index
      expect(index.hash_fields).to eq([tweet['TweetId']].to_set)
      expect(index.order_fields).to eq([])
      expect(index.extra).to eq([
        tweet['Body'],
        tweet['Timestamp'],
        tweet['Retweets']
      ].to_set)
      expect(index.key).to eq 'Tweet'
    end

    context 'when checking validity' do
      it 'cannot have empty hash fields' do
        expect do
          Index.new [], [], [tweet['TweetId']],
                    QueryGraph::Graph.from_path([tweet.id_field])
        end.to raise_error InvalidIndexException
      end

      it 'cannot have hash fields involving multiple entities' do
        expect do
          Index.new [tweet['Body'], user['City']],
                    [tweet.id_field, user.id_field], [],
                    QueryGraph::Graph.from_path([tweet.id_field,
                                                 tweet['User']])
        end.to raise_error InvalidIndexException
      end

      it 'must have fields at the start of the path' do
        expect do
          Index.new [tweet['TweetId']], [], [],
                    QueryGraph::Graph.from_path([tweet.id_field,
                                                 tweet['User']])
        end.to raise_error InvalidIndexException
      end

      it 'must have fields at the end of the path' do
        expect do
          Index.new [user['City']], [], [],
                    QueryGraph::Graph.from_path([tweet.id_field,
                                                 tweet['User']])
        end.to raise_error InvalidIndexException
      end
    end

    context 'when reducing to an ID graph' do
      it 'moves non-ID fields to extra data' do
        index = Index.new [user['City']], [user['UserId']], [],
                          QueryGraph::Graph.from_path([user.id_field])
        id_graph = index.to_id_graph

        expect(id_graph.hash_fields).to match_array [user['UserId']]
        expect(id_graph.order_fields).to be_empty
        expect(id_graph.extra).to match_array [user['City']]
      end

      it 'does not change indexes which are already ID paths' do
        index = Index.new [user['UserId']], [tweet['TweetId']],
                          [tweet['Body']], QueryGraph::Graph.from_path(
                            [user.id_field, user['Tweets']]
                          )
        id_graph = index.to_id_graph

        expect(id_graph).to eq(index)
      end
    end
  end
end
