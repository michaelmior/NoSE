module NoSE
  describe IndexEnumerator do
    include_context 'entities'

    subject(:enum) { IndexEnumerator.new workload }

    it 'produces a simple index for a filter' do
      query = Query.new 'SELECT Username FROM User WHERE User.City = ?',
                        workload.model
      indexes = enum.indexes_for_query query

      expect(indexes.to_a).to include \
        Index.new [user['City']], [], [user['Username']], [user]
    end

    it 'produces a simple index for a foreign key join' do
      query = Query.new 'SELECT Body FROM Tweet.User WHERE User.City = ?',
                        workload.model
      indexes = enum.indexes_for_query query

      expect(indexes).to include \
        Index.new [user['City']], [], [tweet['Body']], [user, tweet]
    end

    it 'produces a simple index for a filter within a workload' do
      query = Query.new 'SELECT Username FROM User WHERE User.City = ?',
                        workload.model
      workload.add_query query
      indexes = enum.indexes_for_workload

      expect(indexes.to_a).to include \
        Index.new [user['City']], [], [user['Username']], [user]
    end

    it 'does not produce empty indexes' do
      query = Query.new 'SELECT Body FROM Tweet.User WHERE User.City = ?',
                        workload.model
      workload.add_query query
      indexes = enum.indexes_for_workload
      expect(indexes).to all(satisfy do |index|
        !index.order_fields.empty? || !index.extra.empty?
      end)
    end
  end
end
