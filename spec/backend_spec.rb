module NoSE
  module Backend
    describe Backend::SortStatementStep do
      include_context 'entities'

      it 'can sort a list of results' do
        results = [
          { 'User_Username' => 'Bob' },
          { 'User_Username' => 'Alice' }
        ]
        step = Plans::SortPlanStep.new [user['Username']]

        step_class = Backend::SortStatementStep
        prepared = step_class.new nil, [], {}, step, nil, nil
        results = prepared.process nil, results

        expect(results).to eq [
          { 'User_Username' => 'Alice' },
          { 'User_Username' => 'Bob' }
        ]
      end
    end

    describe Backend::FilterStatementStep do
      include_context 'entities'

      it 'can filter results by an equality predicate' do
        results = [
          { 'User_Username' => 'Alice' },
          { 'User_Username' => 'Bob' }
        ]
        step = Plans::FilterPlanStep.new [user['Username']], nil
        query = Statement.parse 'SELECT User.* FROM User ' \
                                'WHERE User.Username = "Bob"', workload.model

        step_class = Backend::FilterStatementStep
        prepared = step_class.new nil, [], {}, step, nil, nil
        results = prepared.process query.conditions, results

        expect(results).to eq [
          { 'User_Username' => 'Bob' }
        ]
      end

      it 'can filter results by a range predicate' do
        results = [
          { 'User_Username' => 'Alice' },
          { 'User_Username' => 'Bob' }
        ]
        step = Plans::FilterPlanStep.new [], [user['Username']]
        query = Statement.parse 'SELECT User.* FROM User WHERE ' \
                                'User.Username < "B" AND ' \
                                'User.City = "New York"', workload.model

        step_class = Backend::FilterStatementStep
        prepared = step_class.new nil, [], {}, step, nil, nil
        results = prepared.process query.conditions, results

        expect(results).to eq [
          { 'User_Username' => 'Alice' }
        ]
      end
    end

    describe Backend::FilterStatementStep do
      include_context 'entities'

      it 'can limit results' do
        results = [
          { 'User_Username' => 'Alice' },
          { 'User_Username' => 'Bob' }
        ]
        step = Plans::LimitPlanStep.new 1
        step_class = Backend::LimitStatementStep
        prepared = step_class.new nil, [], {}, step, nil, nil
        results = prepared.process({}, results)

        expect(results).to eq [
          { 'User_Username' => 'Alice' }
        ]
      end
    end
  end
end
