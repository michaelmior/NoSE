module NoSE
  module Cost
    describe RequestCountCost do
      include_context 'entities'

      it 'counts a single request for a single step plan' do
        planner = Plans::QueryPlanner.new workload.model,
                                          [tweet.simple_index], subject
        plan = planner.min_plan \
          Statement.parse 'SELECT Tweet.* FROM Tweet ' \
                          'WHERE Tweet.TweetId = ?', workload.model
        expect(plan.cost).to eq 1
      end
    end

    describe EntityCountCost do
      include_context 'entities'

      it 'counts multiple requests when multiple entities are selected' do
        query = Statement.parse 'SELECT Tweet.* FROM Tweet.User ' \
                                'WHERE User.UserId = ?', workload.model
        planner = Plans::QueryPlanner.new workload.model,
                                          [query.materialize_view], subject
        plan = planner.min_plan query
        expect(plan.cost).to eq 100
      end
    end

    describe FieldSizeCost do
      include_context 'entities'

      it 'measures the size of the selected data' do
        index = tweet.simple_index
        planner = Plans::QueryPlanner.new workload.model, [index], subject
        plan = planner.min_plan \
          Statement.parse 'SELECT Tweet.* FROM Tweet ' \
                          'WHERE Tweet.TweetId = ?', workload.model
        expect(plan.cost).to eq index.all_fields.sum_by(&:size)
      end
    end
  end
end
