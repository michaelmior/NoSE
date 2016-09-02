# frozen_string_literal: true
# rubocop:disable all

NoSE::Model.new do
  # Define entities along with the size and cardinality of their fields
  # as well as an estimated number of each entity

  (Entity 'categories' do
    ID     'id'
    String 'name', 20
    Integer 'dummy', count: 1
  end) * 50

  (Entity 'regions' do
    ID      'id'
    String  'name', 25
    Integer 'dummy', count: 1
  end) * 5

  (Entity 'users' do
    ID         'id'
    String     'firstname', 6
    String     'lastname', 7
    String     'nickname', 12
    String     'password', 15
    String     'email', 23
    Integer    'rating', count: 50
    Float      'balance', count: 10_000
    Date       'creation_date'
  end) * 2_000

  (Entity 'items' do
    ID         'id'
    String     'name', 19
    String     'description', 197
    Float      'initial_price'
    Integer    'quantity', count: 100
    Float      'reserve_price'
    Float      'buy_now'
    Integer    'nb_of_bids', count: 100
    Float      'max_bid'
    Date       'start_date'
    Date       'end_date'
  end) * 20_000

  (Entity 'bids' do
    ID         'id'
    Integer    'qty', count: 5
    Float      'bid'
    Date       'date'
  end) * 200_000

  (Entity 'comments' do
    ID         'id'
    Integer    'rating', count: 10
    Date       'date'
    String     'comment', 130
  end) * 100_000

  (Entity 'buynow' do
    ID         'id'
    Integer    'qty', count: 4
    Date       'date'
  end) * 40_000

  HasOne 'region',       'users',
         'users'      => 'regions'

  HasOne 'seller',       'items_sold',
         'items'      => 'users'

  HasOne 'category',     'items',
         'items'      => 'categories'

  HasOne 'user',         'bids',
         'bids'       => 'users'

  HasOne 'item',         'bids',
         'bids'       => 'items'

  HasOne 'from_user',    'comments_sent',
         'comments'   => 'users'

  HasOne 'to_user',      'comments_received',
         'comments'   => 'users'

  HasOne 'item',         'comments',
         'comments'   => 'items'

  HasOne 'buyer',        'bought_now',
         'buynow'     => 'users'

  HasOne 'item',         'bought_now',
         'buynow'     => 'items'
end

# rubocop:enable all
