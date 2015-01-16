module NoSE
  describe Planner do
    include_context 'entities'

    it 'can look up fields by key' do
      index = tweet.simple_index
      planner = Planner.new(workload.model, [index])
      query = Query.new 'SELECT Body FROM Tweet WHERE Tweet.TweetId = ?',
                        workload.model

      tree = planner.find_plans_for_query query
      expect(tree.first).to eq([IndexLookupPlanStep.new(index)])
      expect(tree).to have(1).plan
      expect(tree.first.cost).to be > 0
    end

    it 'can perform an external sort if an index does not exist' do
      index = tweet.simple_index
      planner = Planner.new(workload.model, [index])
      query = Query.new 'SELECT Body FROM Tweet WHERE Tweet.TweetId = ? ' \
                        'ORDER BY Tweet.Timestamp', workload.model

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
      planner = Planner.new(workload.model, [index])
      query = Query.new 'SELECT Body FROM Tweet WHERE Tweet.TweetId = ? ' \
                        'LIMIT 5', workload.model

      tree = planner.find_plans_for_query query
      expect(tree.first).to eq([IndexLookupPlanStep.new(index)])
      expect(tree).to have(1).plan
      expect(tree.first.last.state.cardinality).to eq 5
    end

    it 'can perform an external sort followed by a limit' do
      index = tweet.simple_index
      planner = Planner.new(workload.model, [index])
      query = Query.new 'SELECT Body FROM Tweet WHERE Tweet.TweetId = ? ' \
                        'ORDER BY Tweet.Timestamp LIMIT 5', workload.model

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
      planner = Planner.new workload.model, []
      query = Query.new 'SELECT Body FROM Tweet WHERE Tweet.TweetId = ?',
                        workload.model
      expect { planner.find_plans_for_query query }.to \
          raise_error NoPlanException
    end

    it 'can find multiple plans' do
      index1 = Index.new [tweet['User']], [tweet['Timestamp']],
                         [tweet['Body']], [tweet]
      index2 = Index.new [tweet['User']], [],
                         [tweet['Timestamp'], tweet['Body']], [tweet]
      planner = Planner.new(workload.model, [index1, index2])
      query = Query.new 'SELECT Body FROM Tweet WHERE Tweet.User = ? ' \
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
      index = Index.new [tweet['TweetId']], [],
                        [tweet['Body'], tweet['Timestamp']], [tweet]
      planner = Planner.new workload.model, [index]
      query = Query.new 'SELECT Body FROM Tweet WHERE Tweet.TweetId = ?',
                        workload.model

      plan = planner.find_plans_for_query(query).first
      expect(plan.last.fields).to include(tweet['TweetId'], tweet['Body'],
                                          tweet['Timestamp'])
    end

    it 'can apply external filtering' do
      index = Index.new [tweet['TweetId']], [],
                        [tweet['Body'], tweet['Timestamp']], [tweet]
      planner = Planner.new workload.model, [index]
      query = Query.new 'SELECT Body FROM Tweet WHERE Tweet.TweetId = ?' \
                        ' AND Tweet.Timestamp > ?', workload.model

      tree = planner.find_plans_for_query(query)
      expect(tree).to have(1).plan
      expect(tree.first.last).to eq FilterPlanStep.new([], tweet['Timestamp'])
    end

    context 'when updating cardinality' do
      before(:each) do
        simple_query = Query.new 'SELECT Body FROM Tweet ' \
                                 'WHERE Tweet.TweetId = ?', workload.model
        @simple_state = QueryState.new simple_query, workload.model

        query = Query.new 'SELECT Body FROM Tweet.User ' \
                          'WHERE User.UserId = ?', workload.model
        @state = QueryState.new query, workload.model
      end

      it 'can reduce the cardinality to 1 when filtering by ID' do
        step = FilterPlanStep.new [workload.model['Tweet']['TweetId']], nil,
                                  @simple_state
        expect(step.state.cardinality).to eq 1
      end

      it 'can apply equality predicates when filtering' do
        step = FilterPlanStep.new [workload.model['Tweet']['Body']], nil,
                                  @simple_state
        expect(step.state.cardinality).to eq 200
      end

      it 'can apply multiple predicates when filtering' do
        step = FilterPlanStep.new [workload.model['Tweet']['Body']],
                                   workload.model['Tweet']['Timestamp'],
                                  @simple_state
        expect(step.state.cardinality).to eq 20
      end

      it 'can apply range predicates when filtering' do
        step = FilterPlanStep.new [], workload.model['Tweet']['Timestamp'],
                                  @simple_state
        expect(step.state.cardinality).to eq 100
      end

      it 'can update the cardinality when performing a lookup' do
        index = Index.new [workload.model['User']['UserId']],
                          [],
                          [workload.model['Tweet']['Body']],
                          [workload.model['Tweet'], workload.model['User']]
        step = IndexLookupPlanStep.new index, @state, RootPlanStep.new(@state)
        expect(step.state.cardinality).to eq 100
      end
    end

    it 'fails if required fields are not available' do
      indexes = [
        Index.new([workload.model['User']['Username']], [],
                  [workload.model['User']['City']], [workload.model['User']]),
        Index.new([workload.model['Tweet']['TweetId']], [],
                  [workload.model['Tweet']['Body']], [workload.model['Tweet']])
      ]
      planner = Planner.new(workload.model, indexes)
      query = Query.new 'SELECT Body FROM Tweet.User WHERE User.Username = ?',
                        workload.model
      expect { planner.find_plans_for_query query }.to \
        raise_error NoPlanException
    end

    it 'can use materialized views which traverse multiple entities' do
      query = Query.new 'SELECT Body FROM Tweet.User WHERE User.Username = ?',
                        workload.model
      workload.add_query query
      indexes = IndexEnumerator.new(workload).indexes_for_workload

      planner = Planner.new workload.model, indexes
      plans = planner.find_plans_for_query(query)
      plan_indexes = plans.map do |plan|
        plan.select { |step| step.is_a? IndexLookupPlanStep }.map(&:index)
      end

      expect(plan_indexes).to include [query.materialize_view]
    end

    it 'can use multiple indices for a query' do
      query = Query.new 'SELECT Body FROM Tweet.User WHERE User.Username = ?',
                        workload.model
      workload.add_query query

      indexes = [
        Index.new([user['Username']], [], [tweet['TweetId']], [user, tweet]),
        Index.new([tweet['TweetId']], [], [tweet['Body']], [tweet])
      ]

      planner = Planner.new workload.model, indexes
      expect(planner.min_plan(query)).to eq [
        IndexLookupPlanStep.new(indexes[0]),
        IndexLookupPlanStep.new(indexes[1])
      ]
    end

    it 'can create plans which visit each entity' do
      query = Query.new 'SELECT URL FROM Link.Tweet.User ' \
                        'WHERE User.Username = ?', workload.model
      workload.add_query query

      indexes = IndexEnumerator.new(workload).indexes_for_workload
      planner = Planner.new workload.model, indexes

      max_steps = planner.find_plans_for_query(query).map(&:length).length
      expect(max_steps).to be >= query.longest_entity_path.length
    end
  end
end
