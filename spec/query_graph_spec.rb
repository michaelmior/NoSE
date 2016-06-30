module NoSE
  module QueryGraph
    describe QueryGraph do
      include_context 'entities'

      context 'when producing subgraphs' do
        it 'produces only itself for a single entity graph' do
          graph = Graph.new(user)
          expect(graph.subgraphs).to match_array [graph]
        end

        it 'produces single node graphs when splitting with two nodes' do
          graph = Graph.new nil, [user, tweet, user['Tweets']]
          subgraphs = graph.subgraphs.to_a
          expect(subgraphs).to match_array [
            graph,
            Graph.new(user),
            Graph.new(tweet)
          ]
        end

        it 'produces all paths when splitting a graph' do
          graph = Graph.new nil,
                            [user, tweet, user['Tweets']],
                            [tweet, link, tweet['Link']]
          subgraphs = graph.subgraphs.to_a
          expect(subgraphs).to match_array [
            graph,
            Graph.new(user),
            Graph.new(tweet),
            Graph.new(link),
            Graph.new(tweet, [user, tweet, user['Tweets']]),
            Graph.new(tweet, [tweet, link, tweet['Link']])
          ]
        end
      end

      context 'when converting to a path' do
        it 'can convert single node graphs' do
          graph = Graph.new user
          expect(graph.to_path).to eq KeyPath.new([user.id_fields.first])
        end

        it 'can convert longer paths' do
          graph = Graph.new user, [user, tweet, user['Tweets']]
          expect(graph.to_path).to eq KeyPath.new([user.id_fields.first,
                                                   user['Tweets']])
        end
      end
    end
  end
end
