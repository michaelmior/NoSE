# frozen_string_literal: true

require_relative '../lib/nose.rb'

NoSE::Schema.new do
  Model 'rubis'

  Index 'users_by_region' do
    Hash    regions.id
    Ordered users.id
    Extra   users.nickname
    Path    regions.id, regions.users
  end

  Index 'user_data' do
    Hash    users.id
    Ordered regions.id
    Extra   users['*'], regions.name
    Path    users.id, users.region
  end

  Index 'user_buynow' do
    Hash    users.id
    Ordered buynow.date, buynow.id, items.id
    Extra   buynow.qty
    Path    users.id, users.bought_now, buynow.item
  end

  Index 'user_items_bid_on' do
    Hash    users.id
    Ordered items.end_date, bids.id, items.id
    Extra   bids.qty
    Path    users.id, users.bids, bids.item
  end

  Index 'user_items_sold' do
    Hash    users.id
    Ordered items.end_date, items.id
    Path    users.id, users.items_sold
  end

  Index 'user_comments_received' do
    Hash    users.id
    Ordered comments.id, items.id
    Extra   comments['*']
    Path    users.id, users.comments_received, comments.item
  end

  Index 'commenter' do
    Hash    comments.id
    Ordered users.id
    Extra   users.nickname
    Path    comments.id, comments.from_user
  end

  Index 'items_with_category' do
    Hash    items.id
    Ordered categories.id
    Extra   items['*']
    Path    items.id, items.category
  end

  Index 'item_bids' do
    Hash    items.id
    Ordered bids.id, users.id
    Extra   items.max_bid, users.nickname, bids.qty, bids.bid, bids.date
    Path    items.id, items.bids, bids.user
  end

  Index 'items_by_category' do
    Hash    categories.id
    Ordered items.end_date, items.id
    Path    categories.id, categories.items
  end

  Index 'category_list' do
    Hash    categories.dummy
    Ordered categories.id
    Extra   categories.name
    Path    categories.id
  end

  Index 'region_list' do
    Hash    regions.dummy
    Ordered regions.id
    Extra   regions.name
    Path    regions.id
  end

  Index 'regions' do
    Hash    regions.id
    Extra   regions.name
    Path    regions.id
  end
end
