module Sadvisor
  describe Planner do
    let(:workload) do
      Workload.new do
        (Entity 'User' do
          ID 'UserId'
          String 'Username'
          String 'City'
        end) * 10

        (Entity 'Tweet' do
          ID         'TweetId'
          String     'Body', 140, count: 5
          Integer    'Timestamp'
          ForeignKey 'User', 'User'
        end) * 1000
      end
    end
    let(:entity) { workload['Tweet'] }

    it 'can look up fields by key' do
      index = entity.simple_index
      planner = Planner.new(workload, [index])
      query = Statement.new 'SELECT Body FROM Tweet', workload

      tree = planner.find_plans_for_query query
      expect(tree.first).to eq([IndexLookupPlanStep.new(index)])
      expect(tree).to have(1).plan
      expect(tree.first.cost).to be > 0
    end

    it 'can perform an external sort if an index does not exist' do
      index = entity.simple_index
      planner = Planner.new(workload, [index])
      query = Statement.new 'SELECT Body FROM Tweet ORDER BY ' \
                            'Tweet.Timestamp', workload

      tree = planner.find_plans_for_query query
      steps = [
        IndexLookupPlanStep.new(index),
        SortPlanStep.new([entity['Timestamp']])
      ]
      expect(tree.first).to eq steps
      expect(tree).to have(1).plan
    end

    it 'raises an exception if there is no plan' do
      planner = Planner.new workload, []
      query = Statement.new 'SELECT Body FROM Tweet', workload
      expect { planner.find_plans_for_query query }.to \
          raise_error NoPlanException
    end

    it 'can find multiple plans' do
      index1 = Index.new [], [entity['Timestamp']], [entity['Body']], [entity]
      index2 = Index.new [entity['TweetId']], [], [entity['Timestamp'],
                         entity['Body']], [entity]
      planner = Planner.new(workload, [index1, index2])
      query = Statement.new 'SELECT Body FROM Tweet ORDER BY Tweet.Timestamp',
                            workload

      tree = planner.find_plans_for_query query
      expect(tree.to_a).to match_array [
        [IndexLookupPlanStep.new(index1)],
        [
          IndexLookupPlanStep.new(index2),
          SortPlanStep.new([entity['Timestamp']])
        ]
      ]
    end

    it 'knows which fields are available at a given step' do
      index = Index.new [entity['TweetId']], [], [entity['Body'],
                        entity['Timestamp']], [entity]
      planner = Planner.new workload, [index]
      query = Statement.new 'SELECT Body FROM Tweet', workload

      plan = planner.find_plans_for_query(query).first
      expect(plan.last.fields).to include(entity['TweetId'], entity['Body'],
                                          entity['Timestamp'])
    end

    it 'can apply external filtering' do
      index = Index.new [entity['TweetId']], [], [entity['Body'],
                        entity['Timestamp']], [entity]
      planner = Planner.new workload, [index]
      query = Statement.new 'SELECT Body FROM Tweet WHERE Tweet.Timestamp > ?',
                            workload

      tree = planner.find_plans_for_query(query)
      expect(tree).to have(1).plan
      expect(tree.first.last).to eq FilterPlanStep.new([], entity['Timestamp'])
    end

    context 'when updating cardinality' do
      before(:each) do
        simple_query = Statement.new 'SELECT Body FROM Tweet ' \
                                     'WHERE Tweet.TweetId = ?', workload
        @simple_state = QueryState.new simple_query, workload

        query = Statement.new 'SELECT Body FROM Tweet ' \
                              'WHERE Tweet.User.UserId = ?', workload
        @state = QueryState.new query, workload
      end

      it 'can reduce the cardinality to 1 when filtering by ID' do
        step = FilterPlanStep.new [workload['Tweet']['TweetId']], nil,
                                  @simple_state
        expect(step.state.cardinality).to eq 1
      end

      it 'can apply equality predicates when filtering' do
        step = FilterPlanStep.new [workload['Tweet']['Body']], nil,
                                  @simple_state
        expect(step.state.cardinality).to eq 200
      end

      it 'can apply multiple predicates when filtering' do
        step = FilterPlanStep.new [workload['Tweet']['Body']],
                                  workload['Tweet']['Timestamp'],
                                  @simple_state
        expect(step.state.cardinality).to eq 20
      end

      it 'can apply range predicates when filtering' do
        step = FilterPlanStep.new [], workload['Tweet']['Timestamp'],
                                  @simple_state
        expect(step.state.cardinality).to eq 100
      end

      it 'can update the cardinality when performing a lookup' do
        index = Index.new [workload['User']['UserId']],
                          [],
                          [workload['Tweet']['Body']],
                          [workload['Tweet'], workload['User']]
        step = IndexLookupPlanStep.new index, @state, RootPlanStep.new(@state)
        expect(step.state.cardinality).to eq 100
      end
    end

    it 'fails if required fields are not available' do
      indexes = [
        Index.new([workload['User']['Username']], [],
                  [workload['User']['City']], [workload['User']]),
        Index.new([workload['Tweet']['TweetId']], [],
                  [workload['Tweet']['Body']], [workload['Tweet']])
      ]
      planner = Planner.new(workload, indexes)
      query = Statement.new 'SELECT Body FROM Tweet ' \
                            'WHERE Tweet.User.Username = ?', workload
      expect { planner.find_plans_for_query query }.to \
        raise_error NoPlanException
    end

    it 'can use materialized views which traverse multiple entities' do
      query = Statement.new 'SELECT Body FROM Tweet ' \
                            'WHERE Tweet.User.Username = ?', workload
      workload.add_query query
      indexes = IndexEnumerator.new(workload).indexes_for_workload

      planner = Planner.new workload, indexes
      plans = planner.find_plans_for_query(query)
      plan_indexes = plans.map do |plan|
        plan.select { |step| step.is_a? IndexLookupPlanStep }.map(&:index)
      end

      expect(plan_indexes).to include [query.materialize_view]
    end
  end
end
