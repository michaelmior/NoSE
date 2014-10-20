# Insipired by the blog post below on data modeling in Cassandra
# www.ebaytechblog.com/2012/07/16/cassandra-data-modeling-best-practices-part-1/

# rubocop:disable all

$workload = Sadvisor::Workload.new do
  # Define entities along with the size and cardinality of their fields
  # as well as an estimated number of each entity

  Entity 'User' do
    ID     'UserID'
    String 'Name', 50
    String 'Email', 50
  end

  Entity 'Item' do
    ID     'ItemID'
    String 'Title', 50
    String 'Desc', 200
  end

  Entity 'Like' do
    ID         'LikeID'
    ForeignKey 'UserID', 'User'
    ForeignKey 'ItemID', 'Item'
    Date       'LikedAt'
  end

  # Define queries and their relative weights
  Q 'SELECT * FROM User WHERE User.UserID = ?'
  Q 'SELECT * FROM Item WHERE Item.ItemID = ?'
  Q 'SELECT * FROM User WHERE User.Like.ItemID = ? ORDER BY User.Like.LikedAt'
  Q 'SELECT * FROM Item WHERE Item.Like.UserID = ? ORDER BY Item.Like.LikedAt'
end

# rubocop:enable all
