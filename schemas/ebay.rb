# frozen_string_literal: true

require_relative '../lib/nose.rb'

NoSE::Schema.new do
  Model 'ebay'

  Index 'users_by_id' do
    Hash  users.UserID
    Extra users['*']
    Path  users.UserID
  end

  Index 'items_by_id' do
    Hash  items.ItemID
    Extra items['*']
    Path  items.ItemID
  end

  Index 'likes_by_user' do
    Hash    users.UserID
    Ordered likes.LikedAt, likes.LikeID, items.ItemID
    Path    users.UserID, users.likes, likes.item
  end

  Index 'likes_by_item' do
    Hash    items.ItemID
    Ordered likes.LikedAt, likes.LikeID, users.UserID
    Path    items.ItemID, items.likes, likes.user
  end
end
