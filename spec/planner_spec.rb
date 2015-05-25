module NoSE::Plans
  describe QueryPlanner do
    include_context 'dummy_cost_model'
    include_context 'entities'

    it 'can look up fields by key' do
      index = tweet.simple_index
      planner = QueryPlanner.new workload.model, [index], cost_model
      query = NoSE::Query.new 'SELECT Tweet.Body FROM Tweet ' \
                              'WHERE Tweet.TweetId = ?', workload.model

      tree = planner.find_plans_for_query query
      expect(tree.first).to eq([IndexLookupPlanStep.new(index)])
      expect(tree).to have(1).plan
      expect(tree.first.cost).to be > 0
    end

    it 'can perform an external sort if an index does not exist' do
      index = NoSE::Index.new [user['City']],
                              [user['UserId'], tweet['TweetId']],
                              [tweet['Timestamp'], tweet['Body']],
                              [user.id_fields.first, user['Tweets']]
      planner = QueryPlanner.new workload.model, [index], cost_model
      query = NoSE::Query.new 'SELECT Tweet.Body FROM Tweet.User WHERE ' \
                              'User.City = ? ORDER BY Tweet.Timestamp',
                              workload.model

      tree = planner.find_plans_for_query query
      steps = [
        IndexLookupPlanStep.new(index),
        SortPlanStep.new([tweet['Timestamp']])
      ]
      expect(tree.first).to eq steps
      expect(tree).to have(1).plan
    end

    it 'can apply a limit directly' do
      index = tweet.simple_index
      planner = QueryPlanner.new workload.model, [index], cost_model
      query = NoSE::Query.new 'SELECT Tweet.Body FROM Tweet WHERE ' \
                              'Tweet.TweetId = ? LIMIT 5', workload.model

      tree = planner.find_plans_for_query query
      expect(tree.first).to eq([IndexLookupPlanStep.new(index)])
      expect(tree).to have(1).plan
      expect(tree.first.last.state.cardinality).to eq 5
    end

    it 'can perform an external sort followed by a limit' do
      index = NoSE::Index.new [user['UserId']], [tweet['TweetId']],
                              [tweet['Timestamp'], tweet['Body']],
                              [user.id_fields.first, user['Tweets']]
      planner = QueryPlanner.new workload.model, [index], cost_model
      query = NoSE::Query.new 'SELECT Tweet.Body FROM Tweet.User WHERE ' \
                              'User.UserId = ? ORDER BY Tweet.Timestamp ' \
                              'LIMIT 5', workload.model

      tree = planner.find_plans_for_query query
      steps = [
        IndexLookupPlanStep.new(index),
        SortPlanStep.new([tweet['Timestamp']]),
        LimitPlanStep.new(5)
      ]
      expect(tree.first).to eq steps
      expect(tree).to have(1).plan
    end

    it 'raises an exception if there is no plan' do
      planner = QueryPlanner.new workload.model, [], cost_model
      query = NoSE::Query.new 'SELECT Tweet.Body FROM Tweet ' \
                              'WHERE Tweet.TweetId = ?', workload.model
      expect { planner.find_plans_for_query query }.to \
          raise_error NoPlanException
    end

    it 'can find multiple plans' do
      index1 = NoSE::Index.new [user['UserId']],
                               [tweet['Timestamp'], tweet['TweetId']],
                               [tweet['Body']],
                               [user.id_fields.first, user['Tweets']]
      index2 = NoSE::Index.new [user['UserId']], [tweet['TweetId']],
                               [tweet['Timestamp'], tweet['Body']],
                               [user.id_fields.first, user['Tweets']]
      planner = QueryPlanner.new workload.model, [index1, index2], cost_model
      query = NoSE::Query.new 'SELECT Tweet.Body FROM Tweet.User ' \
                              'WHERE User.UserId = ? ' \
                              'ORDER BY Tweet.Timestamp', workload.model

      tree = planner.find_plans_for_query query
      expect(tree.to_a).to match_array [
        [IndexLookupPlanStep.new(index1)],
        [
          IndexLookupPlanStep.new(index2),
          SortPlanStep.new([tweet['Timestamp']])
        ]
      ]
    end

    it 'knows which fields are available at a given step' do
      index = NoSE::Index.new [tweet['TweetId']], [],
                             [tweet['Body'], tweet['Timestamp']],
                             [tweet.id_fields.first]
      planner = QueryPlanner.new workload.model, [index], cost_model
      query = NoSE::Query.new 'SELECT Tweet.Body FROM Tweet ' \
                              'WHERE Tweet.TweetId = ?', workload.model

      plan = planner.find_plans_for_query(query).first
      expect(plan.last.fields).to include(tweet['TweetId'], tweet['Body'],
                                          tweet['Timestamp'])
    end

    it 'can apply external filtering' do
      index = NoSE::Index.new [tweet['TweetId']], [],
                              [tweet['Body'], tweet['Timestamp']],
                              [tweet.id_fields.first]
      planner = QueryPlanner.new workload.model, [index], cost_model
      query = NoSE::Query.new 'SELECT Tweet.Body FROM Tweet WHERE ' \
                              'Tweet.TweetId = ? AND Tweet.Timestamp > ?',
                               workload.model

      tree = planner.find_plans_for_query(query)
      expect(tree).to have(1).plan
      expect(tree.first.last).to eq FilterPlanStep.new([], tweet['Timestamp'])
    end

    context 'when updating cardinality' do
      before(:each) do
        simple_query = NoSE::Query.new 'SELECT Tweet.Body FROM Tweet WHERE ' \
                                       'Tweet.TweetId = ?', workload.model
        @simple_state = QueryState.new simple_query, workload.model

        # Pretend we start with all tweets
        @simple_state.cardinality = tweet.count

        query = NoSE::Query.new 'SELECT Tweet.Body FROM Tweet.User ' \
                                'WHERE User.UserId = ?', workload.model
        @state = QueryState.new query, workload.model
      end

      it 'can reduce the cardinality to 1 when filtering by ID' do
        step = FilterPlanStep.new [tweet['TweetId']], nil, @simple_state
        expect(step.state.cardinality).to eq 1
      end

      it 'can apply equality predicates when filtering' do
        step = FilterPlanStep.new [tweet['Body']], nil, @simple_state
        expect(step.state.cardinality).to eq 200
      end

      it 'can apply multiple predicates when filtering' do
        step = FilterPlanStep.new [tweet['Body']], tweet['Timestamp'],
                                  @simple_state
        expect(step.state.cardinality).to eq 20
      end

      it 'can apply range predicates when filtering' do
        step = FilterPlanStep.new [], tweet['Timestamp'], @simple_state
        expect(step.state.cardinality).to eq 100
      end

      it 'can update the cardinality when performing a lookup' do
        index = NoSE::Index.new [user['UserId']], [tweet['TweetId']],
                                [tweet['Body']],
                                [user.id_fields.first, user['Tweets']]
        step = IndexLookupPlanStep.new index, @state, RootPlanStep.new(@state)
        expect(step.state.cardinality).to eq 100
      end
    end

    it 'fails if required fields are not available' do
      indexes = [
        NoSE::Index.new([user['Username']], [user['UserId']], [user['City']],
                        [user.id_fields.first]),
        NoSE::Index.new([tweet['TweetId']], [], [tweet['Body']],
                        [tweet.id_fields.first])
      ]
      planner = QueryPlanner.new workload.model, indexes, cost_model
      query = NoSE::Query.new 'SELECT Tweet.Body FROM Tweet.User ' \
                              'WHERE User.Username = ?', workload.model
      expect { planner.find_plans_for_query query }.to \
        raise_error NoPlanException
    end

    it 'can use materialized views which traverse multiple entities' do
      query = NoSE::Query.new 'SELECT Tweet.Body FROM Tweet.User ' \
                              'WHERE User.Username = ?', workload.model
      workload.add_statement query
      indexes = NoSE::IndexEnumerator.new(workload).indexes_for_workload

      planner = QueryPlanner.new workload.model, indexes, cost_model
      plans = planner.find_plans_for_query(query)
      plan_indexes = plans.map do |plan|
        plan.select { |step| step.is_a? IndexLookupPlanStep }.map(&:index)
      end

      expect(plan_indexes).to include [query.materialize_view]
    end

    it 'can use multiple indices for a query' do
      query = NoSE::Query.new 'SELECT Tweet.Body FROM Tweet.User ' \
                              'WHERE User.Username = ?', workload.model
      workload.add_statement query

      indexes = [
        NoSE::Index.new([user['Username']], [user['UserId'], tweet['TweetId']],
                        [], [user.id_fields.first, user['Tweets']]),
        NoSE::Index.new([tweet['TweetId']], [], [tweet['Body']],
                        [tweet.id_fields.first])
      ]

      planner = QueryPlanner.new workload.model, indexes, cost_model
      expect(planner.min_plan(query)).to eq [
        IndexLookupPlanStep.new(indexes[0]),
        IndexLookupPlanStep.new(indexes[1])
      ]
    end

    it 'can create plans which visit each entity' do
      query = NoSE::Query.new 'SELECT Link.URL FROM Link.Tweets.User ' \
                              'WHERE User.Username = ?', workload.model
      workload.add_statement query

      indexes = NoSE::IndexEnumerator.new(workload).indexes_for_workload
      planner = QueryPlanner.new workload.model, indexes, cost_model

      max_steps = planner.find_plans_for_query(query).map(&:length).length
      expect(max_steps).to be >= query.longest_entity_path.length
    end

    it 'does not use limits for a single entity result set' do
      query = NoSE::Query.new 'SELECT User.* FROM User ' \
                              'WHERE User.UserId = ? ' \
                              'ORDER BY User.UserId LIMIT 10', workload.model
      workload.add_statement query

      indexes = NoSE::IndexEnumerator.new(workload).indexes_for_workload
      planner = QueryPlanner.new workload.model, indexes, cost_model
      plans = planner.find_plans_for_query query

      expect(plans.all? do |plan|
        plan.all? { |step| !step.is_a? LimitPlanStep }
      end).to be_truthy
    end
  end

  describe UpdatePlanner do
    include_context 'dummy_cost_model'
    include_context 'entities'

    it 'can produce a simple plan for an update' do
      update = NoSE::Update.new 'UPDATE User SET City = ? ' \
                                'WHERE User.UserId = ?', workload.model
      index = NoSE::Index.new [tweet['Timestamp']],
                              [tweet['TweetId'], user['UserId']],
                              [user['City']],
                              [tweet.id_fields.first, tweet['User']]
      workload.add_statement update
      indexes = NoSE::IndexEnumerator.new(workload).indexes_for_workload \
        [index]
      planner = NoSE::Plans::QueryPlanner.new workload.model, indexes,
                                              cost_model

      query_plans = {
        update => {
          index => update.support_queries(index).map do |query|
            planner.min_plan(query)
          end
        }
      }
      planner = UpdatePlanner.new workload.model, query_plans, cost_model
      plans = planner.find_plans_for_update update, [index]

      expect(plans).to have(1).item
      update_steps = [
        DeletePlanStep.new(index),
        InsertPlanStep.new(index)
      ]
      expect(plans.first).to eql UpdatePlan.new update, index,
                                                query_plans[update][index],
                                                update_steps, cost_model
    end
  end
end
