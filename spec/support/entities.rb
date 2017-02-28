module NoSE
  RSpec.shared_examples 'entities' do
    let(:workload) do
      # rubocop:disable Lint/Void
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
               'User' =>       'Tweet'

        HasOne 'Link',    'Tweets',
               'Tweet' => 'Link'
      end
    end
    # rubocop:enable Lint/Void

    let(:tweet) { workload.model['Tweet'] }
    let(:user) { workload.model['User'] }
    let(:link) { workload.model['Link'] }
    let(:query) do
      Statement.parse 'SELECT Link.URL FROM Link.Tweets.User ' \
                      'WHERE User.Username = ? LIMIT 5', workload.model
    end

    let(:index) do
      Index.new [user['Username']],
                [tweet['Timestamp'], user['UserId'], tweet['TweetId']],
                [tweet['Body']],
                QueryGraph::Graph.from_path([user.id_field, user['Tweets']]),
                saved_key: 'TweetIndex'
    end

    let(:users) do
      [{
        'User_UserId'   => '18a9a155-c9c7-43b5-9ab0-5967c49f56e9',
        'User_Username' => 'Bob'
      }]
    end

    let(:tweets) do
      [{
        'Tweet_Timestamp' => Time.now,
        'Tweet_TweetId'   => 'e2dee9ee-5297-4f91-a3f7-9dd169008407',
        'Tweet_Body'      => 'This is a test'
      }]
    end

    let(:links) do
      [{
        'Link_LinkId' => '4a5339d8-e619-4ad5-a1be-c0bbceb1cdab',
        'Link_URL' => 'http://www.example.com/'
      }]
    end
  end
end
