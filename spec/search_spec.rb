module Sadvisor
  describe Search do
    include_context 'entities'

    it 'raises an exception if there is no space' do
      workload.add_query 'SELECT TweetId FROM Tweet'
      expect { Search.new(workload).search_overlap(1) }.to raise_error
    end

    it 'produces a materialized view with sufficient space' do
      query = Statement.new 'SELECT UserId FROM User WHERE User.City = ? ' \
                            'ORDER BY User.Username', workload
      workload.add_query query
      indexes = Search.new(workload).search_overlap
      expect(indexes).to include query.materialize_view
    end
  end
end
