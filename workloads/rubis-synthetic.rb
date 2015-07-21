# rubocop:disable all

NoSE::Workload.new do
  # Define entities along with the size and cardinality of their fields
  # as well as an estimated number of each entity

  (Entity 'categories' do
    ID     'id'
    String 'name', 20
  end) * 50

  (Entity 'regions' do
    ID     'id'
    String 'name', 25
  end) * 50

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
  end) * 200_000

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
  end) * 2_000_000

  (Entity 'bids' do
    ID         'id'
    Integer    'qty', count: 5
    Float      'bid'
    Date       'date'
  end) * 20_000_000

  (Entity 'comments' do
    ID         'id'
    Integer    'rating', count: 10
    Date       'date'
    String     'comment', 130
  end) * 10_000_000

  (Entity 'buynow' do
    ID         'id'
    Integer    'qty', count: 4
    Date       'date'
  end) * 4_000_000

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

  # HasOne 'to_user',      'comments_received'
  #        'comments'   => 'users'

  HasOne 'item',         'comments',
         'comments'   => 'items'

  HasOne 'buyer',        'bought_now',
         'buynow'     => 'users'

  HasOne 'item',         'bought_now',
         'buynow'     => 'items'

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
  # Q 'CONNECT comments(?) TO to_user(?)'
  Q 'CONNECT comments(?) TO from_user(?)'
  Q 'CONNECT comments(?) TO item(?)'
end

# rubocop:enable all
