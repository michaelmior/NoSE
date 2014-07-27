$workload = Sadvisor::Workload.new do
  # Define entities along with the size and cardinality of their fields
  # as well as an estimated number of each entity

  (Entity 'categories' do
    ID     'id'
    String('name', 20) * 20
  end) * 20

  (Entity 'regions' do
    ID     'id'
    String('name', 25) * 62
  end) * 62

  (Entity 'users' do
    ID         'id'
    String(    'firstname', 20) * 1001848
    String(    'lastname', 20) * 1001848
    String(    'nickname', 20) * 1001848
    String(    'password', 20) * 1001848
    String(    'email', 20) * 1001848
    Integer(   'rating') * 5
    Float(     'balance') * 1
    Date(      'creation_date') * 10381
    ForeignKey('region', 'regions') * 62
  end) * 1001848

  (Entity 'items' do
    ID         'id'
    String(    'name', 100) * 33721
    String(    'description', 255) * 33721
    Float(     'initial_price') * 4494
    Integer(   'quantity') * 11
    Float(     'reserve_price') * 389
    Float(     'buy_now') * 97
    Integer(   'nb_of_bids') * 15
    Float(     'max_bid') * 2167
    Date(      'start_date') * 1
    Date(      'end_date') * 1
    ForeignKey 'seller', 'users'
    ForeignKey 'category', 'categories'
  end) * 33721

  (Entity 'olditems' do
    ID         'id'
    String(    'name', 100) * 500000
    String(    'description', 255) * 500000
    Float(     'initial_price') * 5000
    Integer(   'quantity') * 10
    Float(     'reserve_price') * 5974
    Float(     'buy_now') * 11310
    Integer(   'nb_of_bids') * 58
    Float(     'max_bid') * 5125
    Date(      'start_date') * 48436
    Date(      'end_date') * 239737
    ForeignKey 'seller', 'users'
    ForeignKey 'category', 'categories'
  end) * 500000

  (Entity 'bids' do
    ID         'id'
    ForeignKey('user_id', 'users') * 993655
    ForeignKey('item_id', 'items') * 426931
    Integer(   'qty') * 10
    Float(     'bid') * 5121
    Date(      'date') * 52913
  end) * 5060576

  (Entity 'comments' do
    ID         'id'
    # ForeignKey('from_user_id', 'users') * 413603
    ForeignKey 'to_user_id', 'users'
    ForeignKey('item_id', 'items') * 443798
    Integer(   'rating') * 5
    Date(      'date') * 51399
    String(    'comment', 255) * 533426
  end) * 533426

  (Entity 'buynow' do
    ID         'id'
    ForeignKey('buyer_id', 'users') * 1519
    ForeignKey('item_id', 'items') * 1549
    Integer(   'qty') * 10
    Date(      'date') * 915
  end) * 1882

  # Define queries and their relative weights

  # BrowseCategories
  Q 'SELECT id, name FROM categories', (4.44 + 3.21)

  # ViewBidHistory
  Q 'SELECT name FROM items WHERE items.id = ?', 2.38 / 4
  Q 'SELECT name FROM olditems WHERE olditems.id = ?', 2.38 / 4
  Q 'SELECT id, user_id, item_id, qty, bid, date FROM bids WHERE bids.item_id = ? ORDER BY bids.date DESC', 2.38 / 4
  Q 'SELECT id, nickname FROM users WHERE users.bids.item_id = ?', 2.38 / 4

  # ViewItem
  Q 'SELECT name FROM items WHERE items.id = ?', 22.95 / 4.0 * 0.75
  Q 'SELECT name FROM olditems WHERE olditems.id = ?', 22.95 / 4.0 * 0.25
  Q 'SELECT bid FROM bids WHERE bids.item_id = ? ORDER BY bids.bid DESC LIMIT 1', 22.95 / 4.0
  Q 'SELECT bid, qty FROM bids WHERE bids.item_id = ? ORDER BY bids.bid DESC LIMIT 5', 22.95 / 4.0
  Q 'SELECT id FROM bids WHERE bids.item_id = ?', 22.95 / 4.0 # XXX: total bids

  # SearchItemsByCategory
  Q 'SELECT id, name, initial_price, max_bid, nb_of_bids, end_date FROM items WHERE items.category = ? AND items.end_date >= ?', (27.77 + 8.26)

  # XXX Not currently supported
  # # SearchItemsByRegion
  # Q 'SELECT id, name, initial_price, max_bid, nb_of_bids, end_date FROM items WHERE items.users.region = ? AND items.category = ? AND items.end_date >= ?', 0.06
  # # BrowseRegions
  # Q 'SELECT id, name FROM regions', (0.03 + 0.02)

  # ViewUserInfo
  Q 'SELECT id, to_user_id, item_id, rating, date, comment FROM comments WHERE comments.to_user_id = ?', 4.41 / 2
  Q 'SELECT id, nickname FROM users WHERE users.id = ?', 4.41 / 2
end
