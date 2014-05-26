$workload = Sadvisor::Workload.new do
  Entity 'Category' do
    ID     'id'
    String 'name', 20
  end

  Entity 'Region' do
    ID     'id'
    String 'name', 25
  end

  Entity 'User' do
    ID         'id'
    String     'firstname', 20
    String     'lastname', 20
    String     'nickname', 20
    String     'email', 50
    Integer    'rating'
    Float      'balance'
    Date       'creation_date'
    ForeignKey 'region', 'Region'
  end

  Entity 'Item' do
    ID         'id'
    String     'name', 100
    String     'description', 255
    Float      'initial_price'
    Integer    'quantity'
    Float      'reserve_price'
    Float      'buy_now'
    Integer    'nb_of_bids'
    Date       'start_date'
    Date       'end_date'
    ForeignKey 'seller', 'User'
    ForeignKey 'category', 'Category'
  end

  Entity 'OldItem' do
    ID         'id'
    String     'name', 100
    String     'description', 255
    Float      'initial_price'
    Integer    'quantity'
    Float      'reserve_price'
    Float      'buy_now'
    Integer    'nb_of_bids'
    Date       'start_date'
    Date       'end_date'
    ForeignKey 'seller', 'User'
    ForeignKey 'category', 'Category'
  end

  Entity 'Bid' do
    ID         'id'
    ForeignKey 'user_id', 'User'
    ForeignKey 'item_id', 'Item'
    Integer    'qty'
    Float      'bid'
    Date       'date'
  end

  Entity 'Comment' do
    ID         'id'
    ForeignKey 'from_user_id', 'User'
    # ForeignKey 'to_user_id', 'User'
    ForeignKey 'item_id', 'Item'
    Integer    'rating'
    Date       'date'
    String     'comment', 255
  end

  Entity 'BuyNow' do
    ID         'id'
    ForeignKey 'buyer_id', 'User'
    ForeignKey 'item_id', 'Item'
    Integer    'qty'
    Date       'date'
  end

  # BrowseCategories
  Q 'SELECT id, name FROM Category', 1.0

  # BrowseRegions
  Q 'SELECT id, name FROM Region', 1.0

  # ViewBidHistory
  Q 'SELECT name FROM Item WHERE Item.id = ?', 1.0
  Q 'SELECT name FROM OldItem WHERE OldItem.id = ?', 1.0
  Q 'SELECT id, user_id, item_id, qty, bid, date FROM Bid WHERE Bid.Item.id = ? ORDER BY Bid.date DESC', 1.0
  Q 'SELECT id, nickname FROM User WHERE User.Bid.Item.id = ?', 1.0

  # ViewItem
  Q 'SELECT name FROM Item WHERE Item.id = ?', 1.0
  Q 'SELECT name FROM OldItem WHERE OldItem.id = ?', 1.0
  Q 'SELECT bid FROM Bid WHERE Bid.Item.id = ? ORDER BY Bid.bid DESC LIMIT 1', 1.0
  Q 'SELECT bid, qty FROM Bid WHERE Bid.Item.id = ? ORDER BY Bid.bid DESC LIMIT 5', 1.0
  Q 'SELECT id FROM Bid WHERE Bid.Item.id = ?', 1.0 # XXX: total bids
end
