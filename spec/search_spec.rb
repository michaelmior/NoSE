module NoSE
  describe Search do
    include_context 'entities'

    it 'raises an exception if there is no space' do
      workload.add_query 'SELECT Body FROM Tweet WHERE Tweet.TweetId = ?'
      indexes = IndexEnumerator.new(workload).indexes_for_workload.to_a
      expect { Search.new(workload).search_overlap(indexes, 1) }.to raise_error
    end

    it 'produces a materialized view with sufficient space', gurobi: true do
      query = Statement.new 'SELECT UserId FROM User WHERE User.City = ? ' \
                            'ORDER BY User.Username', workload
      workload.add_query query

      indexes = IndexEnumerator.new(workload).indexes_for_workload.to_a
      indexes = Search.new(workload).search_overlap indexes
      expect(indexes).to include query.materialize_view
    end

    it 'can perform multiple index lookups on a path segment', gurobi: true do
      query = Statement.new 'SELECT Username FROM User WHERE User.City = ?',
                            workload
      workload.add_query query

      indexes = [
        Index.new([user['City']], [], [user['UserId']], [user]),
        Index.new([user['UserId']], [], [user['Username']], [user])
      ]
      search = Search.new(workload)
      expect do
        search.search_overlap(indexes, indexes.first.size).to_set
      end.to raise_error NoSolutionException
    end
  end
end
