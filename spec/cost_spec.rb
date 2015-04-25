module NoSE::Cost
  describe RequestCountCost do
    include_context 'entities'

    let(:subject) { RequestCountCost }

    it 'counts a single request for a single step plan' do
      planner = NoSE::Plans::QueryPlanner.new workload.model,
                                              [tweet.simple_index], subject
      plan = planner.min_plan \
        NoSE::Query.new('SELECT * FROM Tweet WHERE Tweet.TweetId = ?',
                        workload.model)
      expect(plan.cost).to eq 1
    end
  end
end
