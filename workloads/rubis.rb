$workload = Sadvisor::Workload.new do
  (Entity 'Category' do
    ID     'id'
    String('name', 20) * 20
  end) * 20

  (Entity 'Region' do
    ID     'id'
    String('name', 25) * 62
  end) * 62

  (Entity 'User' do
    ID         'id'
    String(    'firstname', 20) * 1001848
    String(    'lastname', 20) * 1001848
    String(    'nickname', 20) * 1001848
    String(    'password', 20) * 1001848
    String(    'email', 20) * 1001848
    Integer(   'rating') * 5
    Float(     'balance') * 1
    Date(      'creation_date') * 10381
    ForeignKey('region', 'Region') * 62
  end) * 1001848

  (Entity 'Item' do
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
    ForeignKey 'seller', 'User'
    ForeignKey 'category', 'Category'
  end) * 33721

  (Entity 'OldItem' do
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
    ForeignKey 'seller', 'User'
    ForeignKey 'category', 'Category'
  end) * 500000

  (Entity 'Bid' do
    ID         'id'
    ForeignKey('user_id', 'User') * 993655
    ForeignKey('item_id', 'Item') * 426931
    Integer(   'qty') * 10
    Float(     'bid') * 5121
    Date(      'date') * 52913
  end) * 5060576

  (Entity 'Comment' do
    ID         'id'
    ForeignKey('from_user_id', 'User') * 413603
    # ForeignKey 'to_user_id', 'User'
    ForeignKey('item_id', 'Item') * 443798
    Integer(   'rating') * 5
    Date(      'date') * 51399
    String(    'comment', 255) * 533426
  end) * 533426

  (Entity 'BuyNow' do
    ID         'id'
    ForeignKey('buyer_id', 'User') * 1519
    ForeignKey('item_id', 'Item') * 1549
    Integer(   'qty') * 10
    Date(      'date') * 915
  end) * 1882

  # BrowseCategories
  Q 'SELECT id, name FROM Category', 0.11

  # BrowseRegions
  Q 'SELECT id, name FROM Region', (0.03 + 0.02)

  # ViewBidHistory
  Q 'SELECT name FROM Item WHERE Item.id = ?', 0.02 / 4
  Q 'SELECT name FROM OldItem WHERE OldItem.id = ?', 0.02 / 4
  Q 'SELECT id, user_id, item_id, qty, bid, date FROM Bid WHERE Bid.Item.id = ? ORDER BY Bid.date DESC', 0.02 / 4
  Q 'SELECT id, nickname FROM User WHERE User.Bid.Item.id = ?', 0.02 / 4

  # ViewItem
  Q 'SELECT name FROM Item WHERE Item.id = ?', 0.12 / 4.0 * 0.75
  Q 'SELECT name FROM OldItem WHERE OldItem.id = ?', 0.12 / 4.0 * 0.25
  Q 'SELECT bid FROM Bid WHERE Bid.Item.id = ? ORDER BY Bid.bid DESC LIMIT 1', 0.12 / 4.0
  Q 'SELECT bid, qty FROM Bid WHERE Bid.Item.id = ? ORDER BY Bid.bid DESC LIMIT 5', 0.12 / 4.0
  Q 'SELECT id FROM Bid WHERE Bid.Item.id = ?', 0.12 / 4.0 # XXX: total bids

  # SearchItemsByCategory
  Q 'SELECT id, name, initial_price, max_bid, nb_of_bids, end_date FROM Item WHERE Item.Category.id = ? AND Item.end_date >= ?', (0.32 + 0.06)

  # SearchItemsByRegion
  # XXX Not supported
  #Q 'SELECT id, name, initial_price, max_bid, nb_of_bids, end_date FROM Item WHERE Item.User.Region.id = ? AND Item.Category.id = ? AND Item.end_date >= ?', 0.06
end
