module NoSE
  module Search
    describe Search do
      include_context 'dummy cost model'
      include_context 'entities'

      it 'raises an exception if there is no space', solver: true do
        workload.add_statement 'SELECT Tweet.Body FROM Tweet ' \
                               'WHERE Tweet.TweetId = ?'
        indexes = IndexEnumerator.new(workload).indexes_for_workload.to_a
        search = Search.new(workload, cost_model)
        expect do
          search.search_overlap(indexes, 1)
        end.to raise_error(NoSolutionException)
      end

      it 'produces a materialized view with sufficient space', solver: true do
        query = Statement.parse 'SELECT User.UserId FROM User WHERE ' \
                                'User.City = ? ORDER BY User.Username',
                                workload.model
        workload.add_statement query

        indexes = IndexEnumerator.new(workload).indexes_for_workload.to_a
        result = Search.new(workload, cost_model).search_overlap indexes
        indexes = result.indexes
        expect(indexes).to include query.materialize_view
      end

      it 'can perform multiple lookups on a path segment', solver: true do
        query = Statement.parse 'SELECT User.Username FROM User ' \
                                'WHERE User.City = ?', workload.model
        workload.add_statement query

        indexes = [
          Index.new([user['City']], [user['UserId']], [],
                    QueryGraph::Graph.from_path([user.id_field])),
          Index.new([user['UserId']], [], [user['Username']],
                    QueryGraph::Graph.from_path([user.id_field]))
        ]
        search = Search.new(workload, cost_model)
        expect do
          search.search_overlap(indexes, indexes.first.size).to_set
        end.to raise_error NoSolutionException
      end

      it 'does not denormalize heavily updated data', solver: true do
        workload.add_statement 'UPDATE User SET Username = ? ' \
                               'WHERE User.UserId = ?', 0.98
        workload.add_statement 'SELECT User.Username FROM User ' \
                               'WHERE User.City = ?', 0.01
        workload.add_statement 'SELECT User.Username FROM User ' \
                               'WHERE User.Country = ?', 0.01

        # Enumerate the indexes and select those actually used
        indexes = IndexEnumerator.new(workload).indexes_for_workload.to_a
        cost_model = Cost::EntityCountCost.new
        result = Search.new(workload, cost_model).search_overlap indexes
        indexes = result.indexes

        # Get the indexes actually used by the generated plans
        planner = Plans::QueryPlanner.new workload, indexes, cost_model
        plans = workload.queries.map { |query| planner.min_plan query }
        indexes = plans.flat_map(&:indexes).to_set

        expect(indexes).to match_array [
          Index.new([user['Country']], [user['UserId']], [],
                    QueryGraph::Graph.from_path([user.id_field])),
          Index.new([user['City']], [user['UserId']], [],
                    QueryGraph::Graph.from_path([user.id_field])),
          Index.new([user['UserId']], [], [user['Username']],
                    QueryGraph::Graph.from_path([user.id_field]))
        ]
      end
    end
  end
end
