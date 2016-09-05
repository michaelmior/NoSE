# frozen_string_literal: true

# Insipired by the blog post below on data modeling in Cassandra
# www.ebaytechblog.com/2012/07/16/cassandra-data-modeling-best-practices-part-1/

# rubocop:disable all

NoSE::Model.new do
  # Define entities along with the size and cardinality of their fields
  # as well as an estimated number of each entity
  (Entity 'users' do
    ID     'UserID'
    String 'Name', 50
    String 'Email', 50
  end) * 100

  (Entity 'items' do
    ID     'ItemID'
    String 'Title', 50
    String 'Desc', 200
  end) * 1_000

  (Entity 'likes' do
    ID         'LikeID'
    Date       'LikedAt'
  end) * 10_000

  HasOne 'user',    'likes',
         'likes' => 'users'
  HasOne 'item',    'likes',
         'likes' => 'items'
end
