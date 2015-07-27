# rubocop:disable all

NoSE::Model.new do
  # Define entities along with the size and cardinality of their fields
  # as well as an estimated number of each entity

  (Entity 'categories' do
    ID     'id'
    String 'name', 20, count: 20
    Integer 'dummy', count: 1
  end) * 20

  (Entity 'regions' do
    ID     'id'
    String 'name', 25, count: 62
  end) * 62

  (Entity 'users' do
    ID         'id'
    String     'firstname', 20, count: 1001848
    String     'lastname', 20, count: 1001848
    String     'nickname', 20, count: 1001848
    String     'password', 20, count: 1001848
    String     'email', 20, count: 1001848
    Integer    'rating', count: 5
    Float      'balance', count: 1
    Date       'creation_date', count: 10381
  end) * 1001848

  (Entity 'items' do
    ID         'id'
    String     'name', 100, count: 33721
    String     'description', 255, count: 33721
    Float      'initial_price', count: 4494
    Integer    'quantity', count: 11
    Float      'reserve_price', count: 389
    Float      'buy_now', count: 97
    Integer    'nb_of_bids', count: 15
    Float      'max_bid', count: 2167
    Date       'start_date', count: 1
    Date       'end_date', count: 1
  end) * 33721

  (Entity 'bids' do
    ID         'id'
    Integer    'qty', count: 10
    Float      'bid', count: 5121
    Date       'date', count: 52913
  end) * 5060576

  (Entity 'comments' do
    ID         'id'
    Integer    'rating', count: 5
    Date       'date', count: 51399
    String     'comment', 255, count: 533426
  end) * 533426

  (Entity 'buynow' do
    ID         'id'
    Integer    'qty', count: 10
    Date       'date', count: 915
  end) * 1882

  HasOne 'region',      'users',
         'users'     => 'regions', count: 62

  HasOne 'seller',      'items_sold',
         'items'     => 'users'

  HasOne 'category',    'items',
         'items'     => 'categories'

  HasOne 'user',        'bids',
         'bids'      => 'users', count: 993655

  HasOne 'item',        'bids',
         'bids'      => 'items', count: 426931

  # HasOne 'from_user',   'comments_sent',
  #        'comments'  => 'users', count: 41603

  HasOne 'to_user',     'comments_received',
         'comments'  => 'users', count: 443798

  HasOne 'item',        'comments',
         'comments'  => 'items', count: 533426

  HasOne 'buyer',       'bought_now',
         'buynow'    => 'users', count: 1519

  HasOne 'item',        'bought_now',
         'buynow'    => 'items', count: 1549
end

# rubocop:enable all
