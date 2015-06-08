module NoSE::Search
  describe Search do
    include_context 'dummy_cost_model'
    include_context 'entities'

    it 'raises an exception if there is no space' do
      workload.add_statement 'SELECT Tweet.Body FROM Tweet ' \
                             'WHERE Tweet.TweetId = ?'
      indexes = NoSE::IndexEnumerator.new(workload).indexes_for_workload.to_a
      search = Search.new(workload, cost_model)
      expect { search.search_overlap(indexes, 1) }.to raise_error
    end

    it 'produces a materialized view with sufficient space', gurobi: true do
      query = NoSE::Query.new 'SELECT User.UserId FROM User ' \
                              'WHERE User.City = ? ' \
                              'ORDER BY User.Username', workload.model
      workload.add_statement query

      indexes = NoSE::IndexEnumerator.new(workload).indexes_for_workload.to_a
      result = Search.new(workload, cost_model).search_overlap indexes
      indexes = result.indexes
      expect(indexes).to include query.materialize_view
    end

    it 'can perform multiple index lookups on a path segment', gurobi: true do
      query = NoSE::Query.new 'SELECT User.Username FROM User ' \
                              'WHERE User.City = ?', workload.model
      workload.add_statement query

      indexes = [
        NoSE::Index.new([user['City']], [user['UserId']], [],
                        [user.id_fields.first]),
        NoSE::Index.new([user['UserId']], [], [user['Username']],
                        [user.id_fields.first])
      ]
      search = Search.new(workload, cost_model)
      expect do
        search.search_overlap(indexes, indexes.first.size).to_set
      end.to raise_error NoSolutionException
    end

    it 'does not denormalize heavily updated data', gurobi: true do
      workload.add_statement 'UPDATE User SET Username = ? ' \
                             'WHERE User.UserId = ?', 0.98
      workload.add_statement 'SELECT User.Username FROM User ' \
                             'WHERE User.City = ?', 0.01
      workload.add_statement 'SELECT User.Username FROM User ' \
                             'WHERE User.Country = ?', 0.01

      # Enumerate the indexes and select those actually used
      indexes = NoSE::IndexEnumerator.new(workload).indexes_for_workload.to_a
      cost_model = NoSE::Cost::EntityCountCost
      result = Search.new(workload, cost_model).search_overlap indexes
      indexes = result.indexes

      # Get the indexes actually used by the generated plans
      planner = NoSE::Plans::QueryPlanner.new workload, indexes, cost_model
      plans = {}
      workload.queries.each { |query| plans[query] = planner.min_plan query }
      indexes = plans.map(&:to_a).flatten.select do |step|
        step.is_a? NoSE::Plans::IndexLookupPlanStep
      end.map(&:index).to_set

      expect(indexes).to match_array [
        NoSE::Index.new([user['Country']], [user['UserId']], [],
                        [user.id_fields.first]),
        NoSE::Index.new([user['City']], [user['UserId']], [],
                        [user.id_fields.first]),
        NoSE::Index.new([user['UserId']], [],
                        [user['Country'], user['Username']],
                        [user.id_fields.first])
      ]
    end
  end
end
