# frozen_string_literal: true

NoSE::Workload.new do
  Model 'rubis'

  # Define queries and their relative weights

  Q 'SELECT comments.date, comments.comment FROM comments.item ' \
    'WHERE item.id = ? ORDER BY comments.date'
  # 1. SELECT item_id as E_item, date as O_date, from_user_id, date, comment
  #    FROM comments;
  # I2227598752

  Q 'SELECT users.id, users.nickname, users.rating FROM users.region ' \
    'WHERE region.id = ? ORDER BY users.rating LIMIT 50'
  # 2. SELECT region as E_region, rating as O_rating, id, nickname, rating
  #    FROM users;
  # I1083340549

  Q 'SELECT items.id, items.name, items.description, items.max_bid ' \
    'FROM items.seller.region WHERE region.id = ? LIMIT 50'
  # 3. SELECT region as E_region, items.id, name, description, max_bid FROM
  #    items join users on items.seller=users.id WHERE items.seller.region;
  # I4186334592

  Q 'SELECT comments.date, comments.comment FROM ' \
    'comments.item.seller.region WHERE item.quantity = ? AND region.id = ? ' \
    'LIMIT 50'
  # 4. SELECT category AS E_category, region as E_region, from_user_id, date,
  #    comment FROM comments join items on comments.item_id=items.id join
  #    users on items.seller=users.id;
  # I3254083673

  Q 'SELECT bids.bid, bids.date FROM bids.item.seller.region WHERE ' \
    'region.id = ? AND item.quantity = ? AND ' \
    'item.end_date < "2015-06-11T14:00:00-04:00"'
  # 5. SELECT region as E_region, category as E_category,
  #    end_date as O_end_date, bids.id as O_id, bid, date FROM bids join
  #    items on bids.item_id=items.id join users on items.seller=users.id
  # I1184534160

  Q 'SELECT comments.comment, comments.date FROM comments.item.seller ' \
    'WHERE seller.id = ?'
  # 6. SELECT seller AS E_seller, comments.id AS O_id, from_user_id, comment,
  #    date FROM comments join items on comments.item_id=items.id;
  # I638854407

  Q 'SELECT items.id, items.name FROM items.category WHERE category.id = ? ' \
    'LIMIT 1000'
  # 7. SELECT category as E_category, id, name FROM items;
  # I3358488952

  Q 'SELECT comments.comment FROM comments.item.category ' \
    'WHERE category.id = ? ORDER BY comments.date LIMIT 100'
  # 8. SELECT category AS E_category, date AS O_date, comment FROM comments
  #    join items ON comments.item_id=items.id;
  # I127205473

  # RegisterItem
  Q 'INSERT INTO items SET id=?, name=?, description=?, initial_price=?, ' \
    'quantity=?, reserve_price=?, buy_now=?, nb_of_bids=0, max_bid=0, ' \
    'start_date=?, end_date=?'
  Q 'CONNECT items(?) TO category(?)'
  Q 'CONNECT items(?) TO seller(?)'

  # RegisterUser
  Q 'INSERT INTO users SET id=?, firstname=?, lastname=?, nickname=?, ' \
    'password=?, email=?, rating=0, balance=0, creation_date=?'
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
