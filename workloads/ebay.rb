# Insipired by the blog post below on data modeling in Cassandra
# www.ebaytechblog.com/2012/07/16/cassandra-data-modeling-best-practices-part-1/

# rubocop:disable all

NoSE::Workload.new do
  # Define entities along with the size and cardinality of their fields
  # as well as an estimated number of each entity

  Entity 'Users' do
    ID     'UserID'
    String 'Name', 50
    String 'Email', 50
  end

  Entity 'Items' do
    ID     'ItemID'
    String 'Title', 50
    String 'Desc', 200
  end

  Entity 'Likes' do
    ID         'LikeID'
    Date       'LikedAt'
  end

  ManyToOne 'User',    'Likes',
            'Likes' => 'Users'
  ManyToOne 'Item',    'Likes',
            'Likes' => 'Items'

  # Define queries and their relative weights
  Q 'SELECT Users.* FROM Users WHERE Users.UserID = ?'
  Q 'SELECT Items.* FROM Items WHERE Items.ItemID = ?'
  Q 'SELECT Users.* FROM Users.Likes.Item WHERE Item.ItemID = ? ORDER BY Likes.LikedAt'
  Q 'SELECT Items.* FROM Items.Likes.User WHERE User.UserID = ? ORDER BY Likes.LikedAt'
end

# rubocop:enable all
