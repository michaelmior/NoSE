module NoSE
  module QueryGraph
    describe QueryGraph do
      include_context 'entities'

      context 'when producing subgraphs' do
        it 'produces nothing for a single entity graph' do
          graph = Graph.new(user)
          expect(graph.subgraphs).to be_empty
        end

        it 'produces single node graphs when splitting with two nodes' do
          graph = Graph.new nil, [user, tweet, user['Tweets']]
          subgraphs = graph.subgraphs.to_a
          expect(subgraphs).to match_array [
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
            Graph.new(user),
            Graph.new(tweet),
            Graph.new(link),
            Graph.new(tweet, [user, tweet, user['Tweets']]),
            Graph.new(tweet, [tweet, link, tweet['Link']])
          ]
        end
      end
    end
  end
end
