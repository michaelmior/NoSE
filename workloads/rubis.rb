# rubocop:disable all

NoSE::Workload.new do
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

  HasOne 'region',      'users',
         'users'     => 'regions', count: 62

  HasOne 'seller',      'items_sold',
         'items'     => 'users'

  HasOne 'category',    'items',
         'items'     => 'categories'

  HasOne 'seller',      'olditems_sold',
         'olditems'  => 'users'

  HasOne 'category',    'olditems',
         'olditems'  => 'categories'

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

  # Define queries and their relative weights

  # BrowseCategories
  # XXX Must have at least one equality predicate
  Q 'SELECT categories.id, categories.name FROM categories WHERE categories.dummy = 1',
    browsing: (4.44 + 3.21)

  # ViewBidHistory
  Q 'SELECT items.name FROM items WHERE items.id = ?',
    browsing: 2.38 / 4
  Q 'SELECT olditems.name FROM olditems WHERE olditems.id = ?',
    browsing: 2.38 / 4
  Q 'SELECT bids.id, item.id, bids.qty, bids.bid, bids.date FROM bids.item WHERE item.id = ? ORDER BY bids.date',
    browsing: 2.38 / 4
  Q 'SELECT users.id, users.nickname, bids.id FROM users.bids.item WHERE item.id = ?',
    browsing: 2.38 / 4

  # ViewItem
  Q 'SELECT items.* FROM items WHERE items.id = ?',
    browsing: 22.95 / 4.0 * 0.75
  Q 'SELECT olditems.* FROM olditems WHERE olditems.id = ?',
    browsing: 22.95 / 4.0 * 0.25
  Q 'SELECT bids.bid FROM bids.item WHERE item.id = ? ORDER BY bids.bid LIMIT 1',
    browsing: 22.95 / 4.0
  Q 'SELECT bids.bid, bids.qty FROM bids.item WHERE item.id = ? ORDER BY bids.bid LIMIT 5',
    browsing: 22.95 / 4.0
  Q 'SELECT bids.id FROM bids.item WHERE item.id = ?', # XXX: total bids
    browsing: 22.95 / 4.0

  # SearchItemsByCategory
  Q 'SELECT items.id, items.name, items.initial_price, items.max_bid, items.nb_of_bids, items.end_date FROM items.category WHERE category.id = ? AND items.end_date >= ?',
    browsing: (27.77 + 8.26)

  # XXX Not currently supported
  # # SearchItemsByRegion
  # Q 'SELECT id, name, initial_price, max_bid, nb_of_bids, end_date FROM items.users WHERE users.region = ? AND items.category = ? AND items.end_date >= ?', 0.06
  # # BrowseRegions
  # Q 'SELECT id, name FROM regions', (0.03 + 0.02)

  # ViewUserInfo
  Q 'SELECT comments.id, comments.rating, comments.date, comments.comment FROM comments.to_user WHERE to_user.id = ?',
    browsing: 4.41 / 3
  Q 'SELECT comments.id, item.id FROM comments.item WHERE comments.id = ?',
    browsing: 4.41 / 3
  Q 'SELECT to_user.id, to_user.nickname, comments.id FROM comments.to_user WHERE to_user.id = ?',
    browsing: 4.41 / 3

  # RegisterItem
  Q 'INSERT INTO items SET id=?, name=?, description=?, initial_price=?, quantity=?, reserve_price=?, buy_now=?, nb_of_bids=0, max_bid=0, start_date=?, end_date=?'
  Q 'CONNECT items(?) TO category(?)'
  Q 'CONNECT items(?) TO seller(?)'

  # RegisterUser
  Q 'INSERT INTO users SET id=?, firstname=?, lastname=?, nickname=?, password=?, email=?, rating=0, balance=0, creation_date=?'
  Q 'CONNECT users(?) TO region(?)'

  # StoreBid
  Q 'INSERT INTO bids SET id=?, qty=?, bid=?, date=?'
  Q 'CONNECT bids(?) TO item(?)'
  Q 'CONNECT bids(?) TO user(?)'
  Q 'SELECT items.nb_of_bids FROM items WHERE items.id=?'
  Q 'UPDATE items SET nb_of_bids=? WHERE items.id=?'

  # StoreComment
  Q 'UPDATE users SET rating=? WHERE users.id=?'
  Q 'INSERT INTO comments SET id=?, rating=?, date=?, comment=?'
  Q 'CONNECT comments(?) TO to_user(?)'
  # Q 'CONNECT comments(?) TO from_user(?)'
  Q 'CONNECT comments(?) TO item(?)'
end

# rubocop:enable all
