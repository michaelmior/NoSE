module NoSE
  module QueryGraph
    describe QueryGraph do
      include_context 'entities'

      context 'when producing subgraphs' do
        it 'produces only itself for a single entity graph' do
          graph = Graph.new([user])
          expect(graph.subgraphs).to match_array [graph]
        end

        it 'produces single node graphs when splitting with two nodes' do
          graph = Graph.new [], [user, tweet, user['Tweets']]
          subgraphs = graph.subgraphs.to_a
          expect(subgraphs).to match_array [
            graph,
            Graph.new([user]),
            Graph.new([tweet])
          ]
        end

        it 'produces all paths when splitting a graph' do
          graph = Graph.new [], [user, tweet, user['Tweets']],
                            [tweet, link, tweet['Link']]
          subgraphs = graph.subgraphs.to_a
          expect(subgraphs).to match_array [
            graph,
            Graph.new([user]),
            Graph.new([tweet]),
            Graph.new([link]),
            Graph.new([], [user, tweet, user['Tweets']]),
            Graph.new([], [tweet, link, tweet['Link']])
          ]
        end
      end

      context 'when converting to a path' do
        it 'can convert single node graphs' do
          graph = Graph.new [user]
          expect(graph.to_path(user)).to eq KeyPath.new([user.id_fields.first])
        end

        it 'can convert longer paths' do
          graph = Graph.new [], [user, tweet, user['Tweets']]
          expect(graph.to_path(user)).to eq KeyPath.new([user.id_fields.first,
                                                         user['Tweets']])
        end
      end

      context 'when converting from a path' do
        it 'converts empty paths to empty graphs' do
          path = KeyPath.new
          expect(Graph.from_path(path)).to eq Graph.new
        end

        it 'converts single entity paths' do
          path = KeyPath.new [user.id_fields.first]
          expect(Graph.from_path(path)).to eq Graph.new([user])
        end

        it 'converts path with multiple entities' do
          path = KeyPath.new [user.id_fields.first, user['Tweets']]
          expect(Graph.from_path(path)).to eq \
            Graph.new([], [user, tweet, user['Tweets']])
        end
      end
    end
  end
end
