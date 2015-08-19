# Insipired by the blog post below on data modeling in Cassandra
# www.ebaytechblog.com/2012/07/16/cassandra-data-modeling-best-practices-part-1/

# rubocop:disable all

NoSE::Model.new do
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

  HasOne 'User',    'Likes',
         'Likes' => 'Users'
  HasOne 'Item',    'Likes',
         'Likes' => 'Items'
end
