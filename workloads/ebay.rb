# frozen_string_literal: true

# Insipired by the blog post below on data modeling in Cassandra
# www.ebaytechblog.com/2012/07/16/cassandra-data-modeling-best-practices-part-1/

NoSE::Workload.new do
  Model 'ebay'

  # Define queries and their relative weights
  Q 'SELECT users.* FROM users WHERE users.UserID = ? -- 1'
  Q 'SELECT items.* FROM items WHERE items.ItemID = ?'
  Q 'SELECT items.* FROM items.likes.user WHERE user.UserID = ? ORDER BY likes.LikedAt'
  Q 'SELECT users.* FROM users.likes.item WHERE item.ItemID = ? ORDER BY likes.LikedAt'

  Q 'INSERT INTO items SET ItemID = ?, Title = ?, Desc = ?'
  Q 'INSERT INTO users SET UserID = ?, Name = ?, Email = ?'
  Q 'INSERT INTO likes SET LikeID = ?, LikedAt = ? AND CONNECT TO user(?), item(?)'
end
