module NoSE
  module Plans
    describe QueryPlanner do
      include_context 'dummy cost model'
      include_context 'entities'

      it 'can look up fields by key' do
        index = tweet.simple_index
        planner = QueryPlanner.new workload.model, [index], cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet ' \
                                'WHERE Tweet.TweetId = ?', workload.model

        tree = planner.find_plans_for_query query
        expect(tree.first).to eq([IndexLookupPlanStep.new(index)])
        expect(tree).to have(1).plan
        expect(tree.first.cost).to be > 0
      end

      it 'does not use an index with the wrong key path' do
        query = Statement.parse 'SELECT User.Username FROM Tweet.User' \
                                ' WHERE Tweet.TweetId = ?', workload.model
        good_index = query.materialize_view
        bad_index = good_index.dup
        path = KeyPath.new [user.id_field, user['Favourite']]
        bad_index.instance_variable_set :@path, path
        bad_index.instance_variable_set :@graph,
                                        QueryGraph::Graph.from_path(path)

        # With the correct path, this should work
        planner = QueryPlanner.new workload.model, [good_index], cost_model
        expect { planner.find_plans_for_query query }.not_to raise_error

        # With the wrong path, this should fail
        planner = QueryPlanner.new workload.model, [bad_index], cost_model
        expect { planner.find_plans_for_query query }.to \
          raise_error NoPlanException
      end

      it 'can perform an external sort if an index does not exist' do
        index = Index.new [user['City']], [user['UserId'], tweet['TweetId']],
                          [tweet['Timestamp'], tweet['Body']],
                          QueryGraph::Graph.from_path(
                            [user.id_field, user['Tweets']]
                          )
        planner = QueryPlanner.new workload.model, [index], cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                                'WHERE User.City = ? ORDER BY Tweet.Timestamp',
                                workload.model

        tree = planner.find_plans_for_query query
        steps = [
          IndexLookupPlanStep.new(index),
          SortPlanStep.new([tweet['Timestamp']])
        ]
        steps.each { |step| step.calculate_cost cost_model }
        expect(tree.first).to eq steps
        expect(tree).to have(1).plan
      end

      it 'can sort if data on all entities has been fetched' do
        index1 = Index.new [user['UserId']], [tweet['TweetId']],
                           [user['Username']],
                           QueryGraph::Graph.from_path(
                             [user.id_field, user['Tweets']]
                           )
        index2 = Index.new [tweet['TweetId']], [], [tweet['Body']],
                           QueryGraph::Graph.from_path([tweet.id_field])
        planner = QueryPlanner.new workload.model, [index1, index2], cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User WHERE ' \
                                'User.UserId = ? ORDER BY User.Username',
                                workload.model
        expect(planner.min_plan(query)).to eq [
          IndexLookupPlanStep.new(index1),
          SortPlanStep.new([user['Username']]),
          IndexLookupPlanStep.new(index2)
        ]
      end

      it 'can apply a limit directly' do
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                                'WHERE User.UserId = ? LIMIT 5', workload.model
        index = query.materialize_view
        planner = QueryPlanner.new workload.model, [index], cost_model

        tree = planner.find_plans_for_query query
        expect(tree.first).to eq([IndexLookupPlanStep.new(index)])
        expect(tree).to have(1).plan
        expect(tree.first.last.state.cardinality).to eq 5
      end

      it 'can perform an external sort followed by a limit' do
        index = Index.new [user['UserId']], [tweet['TweetId']],
                          [tweet['Timestamp'], tweet['Body']],
                          QueryGraph::Graph.from_path(
                            [user.id_field, user['Tweets']]
                          )
        planner = QueryPlanner.new workload.model, [index], cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                                'WHERE User.UserId = ? ORDER BY ' \
                                'Tweet.Timestamp LIMIT 5', workload.model

        tree = planner.find_plans_for_query query
        steps = [
          IndexLookupPlanStep.new(index),
          SortPlanStep.new([tweet['Timestamp']]),
          LimitPlanStep.new(5)
        ]
        steps.each { |step| step.calculate_cost cost_model }
        expect(tree.first).to eq steps
        expect(tree).to have(1).plan
      end

      it 'raises an exception if there is no plan' do
        planner = QueryPlanner.new workload.model, [], cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet ' \
                                'WHERE Tweet.TweetId = ?', workload.model
        expect { planner.find_plans_for_query query }.to \
          raise_error NoPlanException
      end

      it 'can find multiple plans' do
        index1 = Index.new [user['UserId']],
                           [tweet['Timestamp'], tweet['TweetId']],
                           [tweet['Body']],
                           QueryGraph::Graph.from_path(
                             [user.id_field, user['Tweets']]
                           )
        index2 = Index.new [user['UserId']], [tweet['TweetId']],
                           [tweet['Timestamp'], tweet['Body']],
                           QueryGraph::Graph.from_path(
                             [user.id_field, user['Tweets']]
                           )
        planner = QueryPlanner.new workload.model, [index1, index2], cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User WHERE ' \
                                'User.UserId = ? ORDER BY Tweet.Timestamp',
                                workload.model

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
                          [tweet['Body'], tweet['Timestamp']],
                          QueryGraph::Graph.from_path(
                            [tweet.id_field]
                          )
        planner = QueryPlanner.new workload.model, [index], cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet ' \
                                'WHERE Tweet.TweetId = ?', workload.model

        plan = planner.find_plans_for_query(query).first
        expect(plan.last.fields).to include(tweet['TweetId'], tweet['Body'],
                                            tweet['Timestamp'])
      end

      it 'can apply external filtering' do
        index = Index.new [tweet['TweetId']], [],
                          [tweet['Body'], tweet['Timestamp']],
                          QueryGraph::Graph.from_path(
                            [tweet.id_field]
                          )
        planner = QueryPlanner.new workload.model, [index], cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet WHERE ' \
                                'Tweet.TweetId = ? AND Tweet.Timestamp > ?',
                                workload.model

        tree = planner.find_plans_for_query(query)
        expect(tree).to have(1).plan
        expect(tree.first.last).to eq FilterPlanStep.new([],
                                                         tweet['Timestamp'])
      end

      context 'when updating cardinality' do
        before(:each) do
          simple_query = Statement.parse 'SELECT Tweet.Body FROM ' \
                                         'Tweet WHERE Tweet.TweetId = ?',
                                         workload.model
          @simple_state = QueryState.new simple_query, workload.model

          # Pretend we start with all tweets
          @simple_state.cardinality = tweet.count

          query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
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
          index = Index.new [user['UserId']], [tweet['TweetId']],
                            [tweet['Body']],
                            QueryGraph::Graph.from_path(
                              [user.id_field, user['Tweets']]
                            )
          step = IndexLookupPlanStep.new index, @state,
                                         RootPlanStep.new(@state)
          expect(step.state.cardinality).to eq 100
        end
      end

      it 'fails if required fields are not available' do
        indexes = [
          Index.new([user['Username']], [user['UserId']], [user['City']],
                    QueryGraph::Graph.from_path([user.id_field])),
          Index.new([tweet['TweetId']], [], [tweet['Body']],
                    QueryGraph::Graph.from_path([tweet.id_field]))
        ]
        planner = QueryPlanner.new workload.model, indexes, cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                                'WHERE User.Username = ?', workload.model
        expect { planner.find_plans_for_query query }.to \
          raise_error NoPlanException
      end

      it 'can use materialized views which traverse multiple entities' do
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                                'WHERE User.Username = ?', workload.model
        workload.add_statement query
        indexes = IndexEnumerator.new(workload).indexes_for_workload

        planner = QueryPlanner.new workload.model, indexes, cost_model
        plans = planner.find_plans_for_query(query)
        plan_indexes = plans.map(&:indexes)

        expect(plan_indexes).to include [query.materialize_view]
      end

      it 'can use multiple indices for a query' do
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                                'WHERE User.Username = ?', workload.model
        workload.add_statement query

        indexes = [
          Index.new([user['Username']],
                    [user['UserId'], tweet['TweetId']], [],
                    QueryGraph::Graph.from_path([user.id_field,
                                                user['Tweets']])),
          Index.new([tweet['TweetId']], [], [tweet['Body']],
                    QueryGraph::Graph.from_path([tweet.id_field]))
        ]

        planner = QueryPlanner.new workload.model, indexes, cost_model
        expect(planner.min_plan(query)).to eq [
          IndexLookupPlanStep.new(indexes[0]),
          IndexLookupPlanStep.new(indexes[1])
        ]
      end

      it 'can create plans which visit each entity' do
        query = Statement.parse 'SELECT Link.URL FROM Link.Tweets.User ' \
                                'WHERE User.Username = ?', workload.model
        workload.add_statement query

        indexes = IndexEnumerator.new(workload).indexes_for_workload
        planner = QueryPlanner.new workload.model, indexes, cost_model

        tree = planner.find_plans_for_query(query)
        max_steps = tree.max_by(&:length).length
        expect(max_steps).to be >= query.key_path.length + 1
      end

      it 'does not use limits for a single entity result set' do
        query = Statement.parse 'SELECT User.* FROM User ' \
                                'WHERE User.UserId = ? ' \
                                'ORDER BY User.Username LIMIT 10', workload.model
        workload.add_statement query

        indexes = IndexEnumerator.new(workload).indexes_for_workload
        planner = QueryPlanner.new workload.model, indexes, cost_model
        plans = planner.find_plans_for_query query

        expect(plans).not_to include(a_kind_of(LimitPlanStep))
      end

      it 'uses implicit sorting when the clustering key is filtered' do
        query = Statement.parse 'SELECT Tweets.Body FROM User.Tweets WHERE ' \
                                'User.UserId = ? AND Tweets.Retweets = 0 ' \
                                'ORDER BY Tweets.Timestamp', workload.model
        index = Index.new [user['UserId']], [tweet['Retweets'],
                          tweet['Timestamp'], tweet['TweetId']],
                          [tweet['Body']],
                          QueryGraph::Graph.from_path(
                            [user.id_field, user['Tweets']]
                          )

        planner = QueryPlanner.new workload.model, [index], cost_model
        plan = planner.min_plan query

        expect(plan.steps).not_to include(a_kind_of(SortPlanStep))
      end
    end

    describe UpdatePlanner do
      include_context 'dummy cost model'
      include_context 'entities'

      it 'can produce a simple plan for an update' do
        update = Statement.parse 'UPDATE User SET City = ? ' \
                                 'WHERE User.UserId = ?', workload.model
        index = Index.new [tweet['Timestamp']],
                          [tweet['TweetId'], user['UserId']], [user['City']],
                          QueryGraph::Graph.from_path(
                            [tweet.id_field, tweet['User']]
                          )
        workload.add_statement update
        indexes = IndexEnumerator.new(workload).indexes_for_workload [index]
        planner = Plans::QueryPlanner.new workload.model, indexes, cost_model

        trees = update.support_queries(index).map do |query|
          planner.find_plans_for_query(query)
        end
        planner = UpdatePlanner.new workload.model, trees, cost_model
        plans = planner.find_plans_for_update update, indexes
        plans.each { |plan| plan.select_query_plans indexes }

        update_steps = [
          InsertPlanStep.new(index)
        ]
        plan = UpdatePlan.new update, index, trees, update_steps, cost_model
        plan.select_query_plans indexes
        expect(plans).to match_array [plan]
      end

      it 'can produce a plan with no support queries' do
        update = Statement.parse 'UPDATE User SET City = ? ' \
                                 'WHERE User.UserId = ?', workload.model
        index = Index.new [user['UserId']], [], [user['City']],
                          QueryGraph::Graph.from_path([user.id_field])
        planner = UpdatePlanner.new workload.model, [], cost_model
        plans = planner.find_plans_for_update update, [index]
        plans.each { |plan| plan.select_query_plans [index] }

        expect(plans).to have(1).item
        expect(plans.first.query_plans).to be_empty
      end
    end
  end
end
