module NoSE::Cost
  describe RequestCountCost do
    include_context 'entities'

    let(:subject) { RequestCountCost }

    it 'counts a single request for a single step plan' do
      planner = NoSE::Plans::QueryPlanner.new workload.model,
                                              [tweet.simple_index], subject
      plan = planner.min_plan \
        NoSE::Query.new 'SELECT * FROM Tweet WHERE Tweet.TweetId = ?',
                        workload.model
      expect(plan.cost).to eq 1
    end
  end

  describe EntityCountCost do
    include_context 'entities'

    let(:subject) { EntityCountCost }

    it 'counts multiple requests when multiple entities are selected' do
      query = NoSE::Query.new 'SELECT * FROM Tweet WHERE Tweet.User = ?',
                              workload.model
      planner = NoSE::Plans::QueryPlanner.new workload.model,
                                              [query.materialize_view], subject
      plan = planner.min_plan query
      expect(plan.cost).to eq 100
    end
  end

  describe FieldSizeCost do
    include_context 'entities'

    let(:subject) { FieldSizeCost }

    it 'measures the size of the selected data' do
      index = tweet.simple_index
      planner = NoSE::Plans::QueryPlanner.new workload.model, [index], subject
      plan = planner.min_plan \
        NoSE::Query.new 'SELECT * FROM Tweet WHERE Tweet.TweetId = ?',
                        workload.model
      expect(plan.cost).to eq index.all_fields.map(&:size).inject(&:+)
    end
  end
end
