module NoSE::Search
  describe Search do
    include_context 'dummy_cost_model'
    include_context 'entities'

    it 'raises an exception if there is no space' do
      workload.add_statement 'SELECT Body FROM Tweet WHERE Tweet.TweetId = ?'
      indexes = NoSE::IndexEnumerator.new(workload).indexes_for_workload.to_a
      search = Search.new(workload, cost_model)
      expect { search.search_overlap(indexes, 1) }.to raise_error
    end

    it 'produces a materialized view with sufficient space', gurobi: true do
      query = NoSE::Query.new 'SELECT UserId FROM User WHERE User.City = ? ' \
                              'ORDER BY User.Username', workload.model
      workload.add_statement query

      indexes = NoSE::IndexEnumerator.new(workload).indexes_for_workload.to_a
      indexes = Search.new(workload, cost_model).search_overlap indexes
      expect(indexes).to include query.materialize_view
    end

    it 'can perform multiple index lookups on a path segment', gurobi: true do
      query = NoSE::Query.new 'SELECT Username FROM User WHERE User.City = ?',
                              workload.model
      workload.add_statement query

      indexes = [
        NoSE::Index.new([user['City']], [user['UserId']], [], [user]),
        NoSE::Index.new([user['UserId']], [], [user['Username']], [user])
      ]
      search = Search.new(workload, cost_model)
      expect do
        search.search_overlap(indexes, indexes.first.size).to_set
      end.to raise_error NoSolutionException
    end
  end
end
