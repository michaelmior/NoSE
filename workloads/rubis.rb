# rubocop:disable all

$workload = NoSE::Workload.new do
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

  (Entity 'olditems' do
    ID         'id'
    String     'name', 100, count: 500000
    String     'description', 255, count: 500000
    Float      'initial_price', count: 5000
    Integer    'quantity', count: 10
    Float      'reserve_price', count: 5974
    Float      'buy_now', count: 11310
    Integer    'nb_of_bids', count: 58
    Float      'max_bid', count: 5125
    Date       'start_date', count: 48436
    Date       'end_date', count: 239737
  end) * 500000

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

  ManyToOne 'region',     'users'     => 'regions', count: 62
  ManyToOne 'seller',     'items'     => 'users'
  ManyToOne 'category',   'items'     => 'categories'
  ManyToOne 'seller',     'olditems'  => 'users'
  ManyToOne 'category',   'olditems'  => 'categories'
  ManyToOne 'user_id',    'bids'      => 'users', count: 993655
  ManyToOne 'item_id',    'bids'      => 'items', count: 426931
  #ManyToOne 'from_user_id', 'comments'  => 'users', count: 413603
  ManyToOne 'to_user_id', 'comments'  => 'users', count: 443798
  ManyToOne 'item_id',    'comments'  => 'items', count: 533426
  ManyToOne 'buyer_id',   'buynow'    => 'users', count: 1519
  ManyToOne 'item_id',    'buynow'    => 'items', count: 1549

  # Define queries and their relative weights

  # BrowseCategories
  # XXX Must have at least one equality predicate
  Q 'SELECT id, name FROM categories WHERE categories.dummy = 1', (4.44 + 3.21)

  # ViewBidHistory
  Q 'SELECT name FROM items WHERE items.id = ?', 2.38 / 4
  Q 'SELECT name FROM olditems WHERE olditems.id = ?', 2.38 / 4
  Q 'SELECT id, user_id, item_id, qty, bid, date FROM bids WHERE bids.item_id = ? ORDER BY bids.date', 2.38 / 4
  Q 'SELECT id, nickname FROM users.bids WHERE bids.item_id = ?', 2.38 / 4

  # ViewItem
  Q 'SELECT * FROM items WHERE items.id = ?', 22.95 / 4.0 * 0.75
  Q 'SELECT * FROM olditems WHERE olditems.id = ?', 22.95 / 4.0 * 0.25
  Q 'SELECT bid FROM bids WHERE bids.item_id = ? ORDER BY bids.bid LIMIT 1', 22.95 / 4.0
  Q 'SELECT bid, qty FROM bids WHERE bids.item_id = ? ORDER BY bids.bid LIMIT 5', 22.95 / 4.0
  Q 'SELECT id FROM bids WHERE bids.item_id = ?', 22.95 / 4.0 # XXX: total bids

  # SearchItemsByCategory
  Q 'SELECT id, name, initial_price, max_bid, nb_of_bids, end_date FROM items WHERE items.category = ? AND items.end_date >= ?', (27.77 + 8.26)

  # XXX Not currently supported
  # # SearchItemsByRegion
  # Q 'SELECT id, name, initial_price, max_bid, nb_of_bids, end_date FROM items.users WHERE users.region = ? AND items.category = ? AND items.end_date >= ?', 0.06
  # # BrowseRegions
  # Q 'SELECT id, name FROM regions', (0.03 + 0.02)

  # ViewUserInfo
  Q 'SELECT id, to_user_id, item_id, rating, date, comment FROM comments WHERE comments.to_user_id = ?', 4.41 / 2
  Q 'SELECT id, nickname FROM users WHERE users.id = ?', 4.41 / 2
end

# rubocop:enable all
