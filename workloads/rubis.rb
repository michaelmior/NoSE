# rubocop:disable all

NoSE::Workload.new do
  Model 'rubis'

  # Define queries and their relative weights
  DefaultMix :browsing

  Group 'BrowseCategories', browsing: 4.44 + 3.21, bidding: 7.65 + 5.39 do
    # XXX Must have at least one equality predicate
    Q 'SELECT categories.id, categories.name FROM categories WHERE categories.dummy = 1'
  end

  Group 'ViewBidHistory', browsing: 2.38, bidding: 1.54 do
    Q 'SELECT items.name FROM items WHERE items.id = ?'
    Q 'SELECT bids.id, item.id, bids.qty, bids.bid, bids.date FROM bids.item WHERE item.id = ? ORDER BY bids.date'
    Q 'SELECT users.id, users.nickname, bids.id FROM users.bids.item WHERE item.id = ?'
  end

  Group 'ViewItem', browsing: 22.95, bidding: 14.17 do
    Q 'SELECT items.* FROM items WHERE items.id = ?'
    Q 'SELECT bids.bid, bids.qty FROM bids.item WHERE item.id = ? ORDER BY bids.bid LIMIT 5'
    Q 'SELECT bids.id FROM bids.item WHERE item.id = ?' # XXX: total bids
  end

  Group 'SearchItemsByCategory', browsing: 27.77 + 8.26, bidding: 15.94 + 6.34 do
    Q 'SELECT items.id, items.name, items.initial_price, items.max_bid, items.nb_of_bids, items.end_date FROM items.category WHERE category.id = ? AND items.end_date >= ?'
  end

  # XXX Not currently supported
  # # SearchItemsByRegion
  # Q 'SELECT id, name, initial_price, max_bid, nb_of_bids, end_date FROM items.users WHERE users.region = ? AND items.category = ? AND items.end_date >= ?', 0.06
  # # BrowseRegions
  # Q 'SELECT id, name FROM regions', (0.03 + 0.02)

  Group 'ViewUserInfo', browsing: 4.41, bidding: 2.48 do
    Q 'SELECT comments.id, comments.rating, comments.date, comments.comment FROM comments.to_user WHERE to_user.id = ?'
    Q 'SELECT comments.id, item.id FROM comments.item WHERE comments.id = ?'
    Q 'SELECT to_user.id, to_user.nickname, comments.id FROM comments.to_user WHERE to_user.id = ?'
  end

  Group 'RegisterItem', bidding: 0.53 do
    Q 'INSERT INTO items SET id=?, name=?, description=?, initial_price=?, quantity=?, reserve_price=?, buy_now=?, nb_of_bids=0, max_bid=0, start_date=?, end_date=?'
    Q 'CONNECT items(?) TO category(?)'
    Q 'CONNECT items(?) TO seller(?)'
  end

  Group 'RegisterUser', bidding: 1.07 do
    Q 'INSERT INTO users SET id=?, firstname=?, lastname=?, nickname=?, password=?, email=?, rating=0, balance=0, creation_date=?'
    Q 'CONNECT users(?) TO region(?)'
  end

  Group 'BuyNow', bidding: 1.16 do
    Q 'SELECT users.nickname FROM users WHERE users.id=?'
    Q 'SELECT items.* FROM items WHERE items.id=?'
  end

  Group 'StoreBuyNow', bidding: 1.10 do
    Q 'SELECT users.nickname FROM users WHERE users.id=?'
    Q 'UPDATE items SET quantity=?, end_date=? WHERE items.id=?'
    Q 'INSERT INTO buynow SET id=?, qty=?, date=?'
    Q 'CONNECT buynow(?) TO item(?)'
    Q 'CONNECT buynow(?) TO buyer(?)'
  end

  Group 'PutBid', bidding: 5.40 do
    Q 'SELECT items.* FROM items WHERE items.id=?'
    Q 'SELECT bids.qty, bids.date FROM bids.item WHERE item.id=? ORDER BY bids.bid LIMIT 2'
    Q 'SELECT users.nickname FROM users WHERE users.id=?'
  end

  Group 'StoreBid', bidding: 3.74 do
    Q 'INSERT INTO bids SET id=?, qty=?, bid=?, date=?'
    Q 'CONNECT bids(?) TO item(?)'
    Q 'CONNECT bids(?) TO user(?)'
    Q 'SELECT items.nb_of_bids, items.max_bid FROM items WHERE items.id=?'
    Q 'UPDATE items SET nb_of_bids=?, max_bid=? WHERE items.id=?'
  end

  Group 'PutComment', bidding: 0.46 do
    Q 'SELECT items.* FROM items WHERE items.id=?'
    Q 'SELECT users.* FROM users WHERE users.id=?'
  end

  Group 'StoreComment', bidding: 0.45 do
    Q 'UPDATE users SET rating=? WHERE users.id=?'
    Q 'INSERT INTO comments SET id=?, rating=?, date=?, comment=?'
    Q 'CONNECT comments(?) TO to_user(?)'
    # Q 'CONNECT comments(?) TO from_user(?)'
    Q 'CONNECT comments(?) TO item(?)'
  end

  Group 'AboutMe', bidding: 1.71 do
    Q 'SELECT users.* FROM users WHERE users.id=?'
    Q 'SELECT items.id, items.max_bid FROM items.bids.user WHERE user.id=? AND items.end_date>=?'
    Q 'SELECT items_sold.*, users.nickname FROM users.items_sold WHERE items_sold.id=?'


    # XXX No seller nickname
    Q 'SELECT buynow.* FROM buynow.buyer WHERE buyer.id=? AND buynow.date>=?'
    Q 'SELECT item.* FROM buynow.item WHERE buynow.id=?'

    Q 'SELECT items.* FROM items.seller WHERE seller.id=? AND items.end_date >=?'

    # XXX No received nickname
    Q 'SELECT comments_received.* FROM users.comments_received WHERE users.id = ?'
  end
end

# rubocop:enable all
