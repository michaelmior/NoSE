# rubocop:disable all

NoSE::Workload.new do
  # Define entities along with the size and cardinality of their fields
  # as well as an estimated number of each entity

  (Entity 'categories' do
    ID     'id'
    String 'name', 20
  end) * 500

  (Entity 'regions' do
    ID     'id'
    String 'name', 25
  end) * 50

  (Entity 'users' do
    ID         'id'
    String     'firstname', 20, count: 900_000
    String     'lastname', 20, count: 900_000
    String     'nickname', 20
    String     'password', 20
    String     'email', 20
    Integer    'rating', count: 50
    Float      'balance', count: 10_000
    Date       'creation_date', count: 100_000
  end) * 1_000_000

  (Entity 'items' do
    ID         'id'
    String     'name', 100
    String     'description'
    Float      'initial_price', count: 10_000
    Integer    'quantity', count: 100
    Float      'reserve_price', count: 10_000
    Float      'buy_now', count: 10_000
    Integer    'nb_of_bids', count: 200
    Float      'max_bid', count: 10_000
    Date       'start_date', count: 100_000
    Date       'end_date', count: 100_000
  end) * 10_000_000

  (Entity 'bids' do
    ID         'id'
    Integer    'qty', count: 50
    Float      'bid', count: 10_000
    Date       'date', count: 10_0000
  end) * 100_000_000

  (Entity 'comments' do
    ID         'id'
    Integer    'rating', count: 10
    Date       'date', count: 100_000
    String     'comment', 250
  end) * 50_000_000

  (Entity 'buynow' do
    ID         'id'
    Integer    'qty', count: 50
    Date       'date', count: 100_000
  end) * 20_00_000

  HasOne 'region',       'users',
         'users'      => 'regions'

  HasOne 'seller',       'items_sold',
         'items'      => 'users'

  HasOne 'category',     'items',
         'items'      => 'categories'

  HasOne 'user',         'bids',
         'bids'       => 'users', count: 900_000

  HasOne 'item',         'bids',
         'bids'       => 'items', count: 90_000_000

  HasOne 'from_user',    'comments_sent',
         'comments'   => 'users', count: 500_000

  # HasOne 'to_user',      'comments_received'
  #        'comments'   => 'users'

  HasOne 'item',         'comments',
         'comments'   => 'items'

  HasOne 'buyer',        'bought_now',
         'buynow'     => 'users', count: 500_000

  HasOne 'item',         'bought_now',
         'buynow'     => 'items', count: 5_000_000

  # Define queries and their relative weights

  Q 'SELECT comments.date, comments.comment FROM comments.item WHERE item.id = ? ORDER BY comments.date'
  # 1. SELECT item_id as E_item, date as O_date, from_user_id, date, comment FROM comments;
  # I2227598752

  Q 'SELECT users.id, users.nickname, users.rating FROM users.region WHERE region.id = ? ORDER BY users.rating LIMIT 10'
  # 2. SELECT region as E_region, rating as O_rating, id, nickname, rating FROM users;
  # I1083340549

  Q 'SELECT items.id, items.name, items.description, items.max_bid FROM items.seller.region WHERE region.id = ?'
  # 3. SELECT region as E_region, items.id, name, description, max_bid FROM items join users on items.seller=users.id WHERE items.seller.region;
  # I4186334592

  Q 'SELECT comments.date, comments.comment FROM comments.item.seller.region WHERE item.quantity = ? AND region.id = ?'
  # 4. SELECT category AS E_category, region as E_region, from_user_id, date, comment FROM comments join items on comments.item_id=items.id join users on items.seller=users.id;
  # I3254083673

  Q 'SELECT bids.bid, bids.date FROM bids.item.seller.region WHERE region.id = ? AND item.quantity = ? AND item.end_date < ?'
  # 5. SELECT region as E_region, category as E_category, end_date as O_end_date, bids.id as O_id, bid, date FROM bids join items on bids.item_id=items.id join users on items.seller=users.id
  # I1184534160

  Q 'SELECT comments.comment, comments.date FROM comments.item.seller WHERE seller.id = ?'
  # 6. SELECT seller AS E_seller, comments.id AS O_id, from_user_id, comment, date FROM comments join items on comments.item_id=items.id;
  # I638854407

  Q 'SELECT items.id, items.name FROM items.category WHERE category.id = ? LIMIT 1000'
  # 7. SELECT category as E_category, id, name FROM items;
  # I3358488952

  Q 'SELECT comments.comment FROM comments.item.category WHERE category.id = ? ORDER BY comments.date'
  # 8. SELECT category AS E_category, date AS O_date, comment FROM comments join items ON comments.item_id=items.id;
  # I127205473
end

# rubocop:enable all
