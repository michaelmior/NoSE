# Insipired by the blog post below on data modeling in Cassandra
# www.ebaytechblog.com/2012/07/16/cassandra-data-modeling-best-practices-part-1/

# rubocop:disable all

$workload = NoSE::Workload.new do
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
    Date       'LikedAt'
  end

  OneToMany 'UserID', 'Like' => 'User'
  OneToMany 'ItemID', 'Like' => 'Item'

  # Define queries and their relative weights
  Q 'SELECT * FROM User WHERE User.UserID = ?'
  Q 'SELECT * FROM Item WHERE Item.ItemID = ?'
  Q 'SELECT * FROM User.Like WHERE Like.ItemID = ? ORDER BY Like.LikedAt'
  Q 'SELECT * FROM Item.Like WHERE Like.UserID = ? ORDER BY Like.LikedAt'
end

# rubocop:enable all
