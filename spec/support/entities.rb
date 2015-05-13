module NoSE
  RSpec.shared_examples 'entities' do
    let(:workload) do
      Workload.new do
        (Entity 'User' do
          ID     'UserId'
          String 'Username', 10
          String 'City'

          etc
        end) * 10

        Entity 'Link' do
          ID     'LinkId'
          String 'URL'
        end

        (Entity 'Tweet' do
          ID         'TweetId'
          String     'Body', 140, count: 5
          Integer    'Timestamp'
        end) * 1000

        ManyToOne 'User', 'Tweet' => 'User'
        ManyToOne 'Link', 'Tweet' => 'Link'
      end
    end
    let(:tweet) { workload.model['Tweet'] }
    let(:user) { workload.model['User'] }
    let(:link) { workload.model['Link'] }
    let(:query) do
      Query.new 'SELECT Link.URL FROM Link.Tweet.User ' \
                'WHERE User.Username = ? LIMIT 5', workload.model
    end
  end
end
