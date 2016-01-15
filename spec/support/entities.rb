module NoSE
  RSpec.shared_examples 'entities' do
    let(:workload) do
      # rubocop:disable Style/SingleSpaceBeforeFirstArg, Lint/Void
      Workload.new do
        (Entity 'User' do
          ID     'UserId'
          String 'Username', 10
          String 'City', count: 5
          String 'Country'

          etc
        end) * 10

        (Entity 'Link' do
          ID     'LinkId'
          String 'URL'
        end) * 100

        (Entity 'Tweet' do
          ID         'TweetId'
          String     'Body', 140, count: 5
          Date       'Timestamp'
          Integer    'Retweets'
        end) * 1000

        HasOne 'User',    'Tweets',
               'Tweet' => 'User'

        HasOne 'Favourite',    'Favourited',
               'User'       => 'Tweet'

        HasOne 'Link',    'Tweets',
               'Tweet' => 'Link'
      end
    end
    # rubocop:enable Style/SingleSpaceBeforeFirstArg, Lint/Void

    let(:tweet) { workload.model['Tweet'] }
    let(:user) { workload.model['User'] }
    let(:link) { workload.model['Link'] }
    let(:query) do
      Query.new 'SELECT Link.URL FROM Link.Tweets.User ' \
                'WHERE User.Username = ? LIMIT 5', workload.model
    end

    let(:index) do
      NoSE::Index.new [user['Username']],
                      [tweet['Timestamp'], user['UserId'], tweet['TweetId']],
                      [tweet['Body']],
                      [user.id_fields.first, user['Tweets']], 'TweetIndex'
    end
  end
end
