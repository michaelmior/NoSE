module Sadvisor
  describe Planner do
    before(:each) do
      @entity = Entity.new('Tweet') * 1000
      @id_field = IDField.new('TweetId')
      @entity << @id_field
      @body_field = StringField.new('Body', 140) * 5
      @entity << @body_field

      @time_field = IntegerField.new('Timestamp')
      @entity << @time_field

      @other = Entity.new('User') * 10
      @other << IDField.new('UserId')
      @entity << ForeignKey.new('User', @other)

      @workload = Workload.new
      @workload.add_entity @entity
      @workload.add_entity @other
    end

    it 'can look up fields by key' do
      index = @entity.simple_index
      planner = Planner.new(@workload, [index])
      query = Statement.new 'SELECT Body FROM Tweet', @workload

      tree = planner.find_plans_for_query query
      expect(tree.first).to eq([IndexLookupStep.new(index)])
      expect(tree).to have(1).plan
      expect(tree.first.cost).to be > 0
    end

    it 'can perform an external sort if an index does not exist' do
      index = @entity.simple_index
      planner = Planner.new(@workload, [index])
      query = Statement.new 'SELECT Body FROM Tweet ORDER BY ' \
                            'Tweet.Timestamp', @workload

      tree = planner.find_plans_for_query query
      steps = [IndexLookupStep.new(index), SortStep.new([@time_field])]
      expect(tree.first).to eq steps
      expect(tree).to have(1).plan
    end

    it 'raises an exception if there is no plan' do
      planner = Planner.new(@workload, [])
      query = Statement.new 'SELECT Body FROM Tweet', @workload
      expect { planner.find_plans_for_query query }.to \
          raise_error NoPlanException
    end

    it 'can find multiple plans' do
      index1 = Index.new [], [@time_field], [@body_field], [@entity]
      index2 = Index.new [@id_field], [], [@time_field, @body_field], [@entity]
      planner = Planner.new(@workload, [index1, index2])
      query = Statement.new 'SELECT Body FROM Tweet ORDER BY Tweet.Timestamp',
                            @workload

      tree = planner.find_plans_for_query query
      expect(tree.to_a).to match_array [
        [IndexLookupStep.new(index1)],
        [IndexLookupStep.new(index2), SortStep.new([@time_field])]
      ]
    end

    it 'knows which fields are available at a given step' do
      index = Index.new [@id_field], [], [@body_field, @time_field], [@entity]
      planner = Planner.new(@workload, [index])
      query = Statement.new 'SELECT Body FROM Tweet', @workload

      plan = planner.find_plans_for_query(query).first
      expect(plan.last.fields).to include(@id_field, @body_field, @time_field)
    end

    it 'can apply external filtering' do
      index = Index.new [@id_field], [], [@body_field, @time_field], [@entity]
      planner = Planner.new(@workload, [index])
      query = Statement.new 'SELECT Body FROM Tweet WHERE Tweet.Timestamp > ?',
                            @workload

      tree = planner.find_plans_for_query(query)
      expect(tree).to have(1).plan
      expect(tree.first.last).to eq FilterStep.new([], @time_field)
    end

    context 'when updating cardinality' do
      before(:each) do
        simple_query = Statement.new 'SELECT Body FROM Tweet ' \
                                     'WHERE Tweet.TweetId = ?', @workload
        @simple_state = QueryState.new simple_query, @workload

        query = Statement.new 'SELECT Body FROM Tweet ' \
                              'WHERE Tweet.User.UserId = ?', @workload
        @state = QueryState.new query, @workload
      end

      it 'can reduce the cardinality to 1 when filtering by ID' do
        step = FilterStep.new [@workload['Tweet']['TweetId']], nil,
                              @simple_state
        expect(step.state.cardinality).to eq 1
      end

      it 'can apply equality predicates when filtering' do
        step = FilterStep.new [@workload['Tweet']['Body']], nil,
                              @simple_state
        expect(step.state.cardinality).to eq 200
      end

      it 'can apply multiple predicates when filtering' do
        step = FilterStep.new [@workload['Tweet']['Body']],
                              @workload['Tweet']['Timestamp'],
                              @simple_state
        expect(step.state.cardinality).to eq 20
      end

      it 'can apply range predicates when filtering' do
        step = FilterStep.new [], @workload['Tweet']['Timestamp'],
                              @simple_state
        expect(step.state.cardinality).to eq 100
      end

      it 'can update the cardinality when performing a lookup' do
        index = Index.new [@workload['User']['UserId']],
                          [],
                          [@workload['Tweet']['Body']],
                          [@workload['Tweet'], @workload['User']]
        step = IndexLookupStep.new index, @state, RootStep.new(@state)
        expect(step.state.cardinality).to eq 100
      end
    end
  end
end
