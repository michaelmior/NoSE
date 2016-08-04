require 'nose/backend/file'

module NoSE
  module Backend
    describe FileBackend do
      include_context 'dummy_cost_model'
      include_context 'entities'

      # Prepare an update to modify an index with a given
      # set of indexes which are usable for support queries
      # @return [PreparedUpdate]
      def prepare_update_for_backend(statement, index, all_indexes)
        # Plan all the support queries
        planner = Plans::QueryPlanner.new workload.model, all_indexes,
                                          cost_model
        trees = statement.support_queries(index).map do |query|
          planner.find_plans_for_query(query)
        end

        # Plan the update
        planner = Plans::UpdatePlanner.new workload.model, trees, cost_model
        plans = planner.find_plans_for_update statement, all_indexes
        plans.each { |plan| plan.select_query_plans all_indexes }
        plans.select! { |plan| plan.index == index }

        # Prepare the statement
        backend.prepare(statement, plans).first
      end

      let(:tweets_by_user) do
        Index.new [user['UserId']], [tweet['TweetId']],
                  [tweet['Timestamp']],
                  QueryGraph::Graph.from_path([user.id_field, user['Tweets']]),
                  'TweetsByUser'
      end

      let(:users_by_name) do
        Index.new [user['Username']], [user['UserId']], [],
                  QueryGraph::Graph.from_path([user.id_field]),
                  'UsersByName'
      end

      let(:index_data) do
        tweets_and_users = users.product(tweets).map { |h| h.reduce(&:merge) }
        {
          user.simple_index.key => users,
          tweet.simple_index.key => tweets,
          index.key => tweets_and_users,
          users_by_name.key => users,
          tweets_by_user.key => tweets_and_users
        }
      end

      let(:backend) do
        backend = FileBackend.new workload, [index], [], [], {}

        backend.instance_variable_set :@index_data, index_data

        backend
      end

      let(:query) do
        Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                        'WHERE User.Username = "Bob" ' \
                        'ORDER BY Tweet.Timestamp LIMIT 10', workload.model
      end

      it 'uses index descriptions for ddl' do
        expect(backend.indexes_ddl).to match_array [
          index.inspect
        ]
      end

      it 'can look up results based on a query plan' do
        # Materialize a view for the given query
        index = query.materialize_view
        planner = Plans::QueryPlanner.new workload.model, [index], cost_model

        # Execute the planned query
        step = planner.min_plan(query).first
        index_data = { index.key => [{
          'User_Username' => users.first['User_Username']
        }] }
        step_class = FileBackend::IndexLookupStatementStep
        prepared = step_class.new index_data, query.all_fields,
                                  query.conditions, step, nil, step.parent
        results = prepared.process query.conditions, nil

        # Verify we get the result we started with
        expect(results).to eq index_data[index.key]
      end

      it 'can insert into an index' do
        index = link.simple_index
        links.first['Link_LinkId'] = nil

        index_data = { index.key => [] }
        step_class = FileBackend::InsertStatementStep
        prepared = step_class.new index_data, index,
                                  [link['LinkId'], link['URL']]
        prepared.process links

        # Validate the inserted data
        data = index_data[index.key]
        expect(data).to have(1).item
        expect(data.first).to have_key 'Link_LinkId'
        expect(data.first['Link_LinkId']).not_to be_nil
        expect(data.first['Link_URL']).to eq links.first['Link_URL']
      end

      it 'can prepare a query' do
        planner = Plans::QueryPlanner.new workload.model, [index], cost_model
        plan = planner.min_plan(query)
        prepared = backend.prepare query, [plan]

        expect(prepared.steps).to have(1).item
        expect(prepared.steps.first).to be_a \
          FileBackend::IndexLookupStatementStep

        result = prepared.execute(
          'User_Username' => Condition.new(user['Username'], :'=',
                                           users.first['User_Username'])
        )

        expect(result).to eq [{ 'Tweet_Body' => tweets.first['Tweet_Body'] }]
      end

      context 'when performing deletes' do
        it 'can delete by ID' do
          delete = Statement.parse 'DELETE User FROM User ' \
                                   'WHERE User.UserId = ?', workload.model
          indexes = [user.simple_index, tweet.simple_index,
                     index, tweets_by_user]

          prepared = prepare_update_for_backend delete, index, indexes

          prepared.execute(
            [],
            'User_UserId' => Condition.new(user['UserId'], :'=',
                                           users.first['User_UserId'])
          )

          expect(index_data['TweetIndex']).to be_empty
        end

        it 'does not delete if the ID does not match' do
          delete = Statement.parse 'DELETE User FROM User ' \
                                   'WHERE User.UserId = ?', workload.model
          indexes = [user.simple_index, tweet.simple_index,
                     index, tweets_by_user]

          prepared = prepare_update_for_backend delete, index, indexes

          prepared.execute(
            [],
            'User_UserId' => Condition.new(user['UserId'], :'=', 'NOT_HERE')
          )

          expect(index_data['TweetIndex']).to have(1).item
        end

        it 'can delete by other attributes' do
          delete = Statement.parse 'DELETE User FROM User ' \
                                   'WHERE User.Username = ?', workload.model
          indexes = [user.simple_index, tweet.simple_index,
                     index, users_by_name, tweets_by_user]

          prepared = prepare_update_for_backend delete, index, indexes

          prepared.execute(
            [],
            'User_Username' => Condition.new(user['Username'], :'=',
                                             users.first['User_Username'])
          )

          expect(index_data['TweetIndex']).to be_empty
        end
      end
    end
  end
end
