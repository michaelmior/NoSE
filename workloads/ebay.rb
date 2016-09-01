# Insipired by the blog post below on data modeling in Cassandra
# www.ebaytechblog.com/2012/07/16/cassandra-data-modeling-best-practices-part-1/

NoSE::Workload.new do
  Model 'ebay'

  # Define queries and their relative weights
  Q 'SELECT Users.* FROM Users WHERE Users.UserID = ? -- 1'
  Q 'SELECT Items.* FROM Items WHERE Items.ItemID = ?'
  Q 'SELECT Items.* FROM Items.Likes.User WHERE User.UserID = ? ORDER BY Likes.LikedAt'
  Q 'SELECT Users.* FROM Users.Likes.Item WHERE Item.ItemID = ? ORDER BY Likes.LikedAt'
end
