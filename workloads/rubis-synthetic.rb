# rubocop:disable all

$workload = Sadvisor::Workload.new do
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
    ForeignKey 'region', 'regions', count: 62
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
    ForeignKey 'seller', 'users'
    ForeignKey 'category', 'categories'
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
    ForeignKey 'seller', 'users'
    ForeignKey 'category', 'categories'
  end) * 500000

  (Entity 'bids' do
    ID         'id'
    ForeignKey 'user', 'users', count: 993655
    ForeignKey 'item_id', 'items', count: 426931
    Integer    'qty', count: 10
    Float      'bid', count: 5121
    Date       'date', count: 52913
  end) * 5060576

  (Entity 'comments' do
    ID         'id'
     ForeignKey 'from_user_id', 'users', count: 413603
    #ForeignKey 'to_user_id', 'users'
    ForeignKey 'item_id', 'items', count: 443798
    Integer    'rating', count: 5
    Date       'date', count: 51399
    String     'comment', 255, count: 533426
  end) * 533426

  (Entity 'buynow' do
    ID         'id'
    ForeignKey 'buyer', 'users', count: 1519
    ForeignKey 'item', 'items', count: 1549
    Integer    'qty', count: 10
    Date       'date', count: 915
  end) * 1882

  # Define queries and their relative weights

  Q 'SELECT from_user_id, date, comment FROM comments WHERE comments.item_id = ? ORDER BY comments.date'
  # 1. SELECT item_id as E_item, date as O_date, from_user_id, date, comment FROM comments;
  # I2227598752

  Q 'SELECT id, nickname, rating FROM users WHERE users.region = ? ORDER BY users.rating LIMIT 10'
  # 2. SELECT region as E_region, rating as O_rating, id, nickname, rating FROM users;
  # I1083340549

  Q 'SELECT id, name, description, max_bid FROM items WHERE items.seller.region = ?'
  # 3. SELECT region as E_region, items.id, name, description, max_bid FROM items join users on items.seller=users.id WHERE items.seller.region;
  # I4186334592

  Q 'SELECT from_user_id, date, comment FROM comments WHERE comments.item_id.category = ? AND comments.item_id.seller.region = ?'
  # 4. SELECT category AS E_category, region as E_region, from_user_id, date, comment FROM comments join items on comments.item_id=items.id join users on items.seller=users.id;
  # I3254083673

  Q 'SELECT bid, date FROM bids WHERE bids.item_id.seller.region = ? AND bids.item_id.category = ? AND bids.item_id.end_date < ?'
  # 5. SELECT region as E_region, category as E_category, end_date as O_end_date, bids.id as O_id, bid, date FROM bids join items on bids.item_id=items.id join users on items.seller=users.id
  # I1184534160

  Q 'SELECT from_user_id, comment, date FROM comments WHERE comments.item_id.seller = ?'
  # 6. SELECT seller AS E_seller, comments.id AS O_id, from_user_id, comment, date FROM comments join items on comments.item_id=items.id;
  # I638854407

  Q 'SELECT id, name FROM items WHERE items.category = ?'
  # 7. SELECT category as E_category, id, name FROM items;
  # I3358488952

  Q 'SELECT comment FROM comments WHERE comments.item_id.category = ? ORDER BY comments.date'
  # 8. SELECT category AS E_category, date AS O_date, comment FROM comments join items ON comments.item_id=items.id;
  # I127205473
end

# rubocop:enable all
