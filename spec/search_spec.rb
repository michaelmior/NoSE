module Sadvisor
  describe Search do
    include_context 'entities'

    it 'raises an exception if there is no space' do
      workload.add_query 'SELECT Body FROM Tweet WHERE Tweet.TweetId = ?'
      expect { Search.new(workload).search_overlap(1) }.to raise_error
    end

    it 'produces a materialized view with sufficient space' do
      query = Statement.new 'SELECT UserId FROM User WHERE User.City = ? ' \
                            'ORDER BY User.Username', workload
      workload.add_query query

      indexes = Search.new(workload).search_overlap
      expect(indexes).to include query.materialize_view
    end

    it 'can allow for multiple index lookups on one path segment' do
      query = Statement.new 'SELECT Username FROM User WHERE User.City = ?',
                            workload
      workload.add_query query

      indexes = [
        Index.new([user['City']], [], [user['UserId']], [user]),
        Index.new([user['UserId']], [], [user['Username']], [user])
      ]
      search = Search.new(workload)
      expect do
        search.search_overlap(indexes.first.size, indexes: indexes).to_set
      end.to raise_error NoSolutionException
    end
  end
end
