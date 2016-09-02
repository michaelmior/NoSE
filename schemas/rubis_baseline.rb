# frozen_string_literal: true

require_relative '../lib/nose.rb'

NoSE::Schema.new do
  Model 'rubis'

  SimpleIndex 'categories'
  SimpleIndex 'regions'
  SimpleIndex 'items'
  SimpleIndex 'comments'

  Index 'users_by_region' do
    Hash    regions.id
    Ordered users.id
    Extra   users.nickname
    Path    regions.id, regions.users
  end

  Index 'users' do
    Hash    users.id
    Ordered regions.id
    Extra   users['*']
    Path    users.id, users.region
  end

  Index 'bids' do
    Hash    bids.id
    Ordered users.id, items.id
    Extra   bids['*']
    Path    users.id, users.bids, bids.item
  end

  Index 'buynow' do
    Hash    buynow.id
    Ordered items.id
    Extra   buynow['*']
    Path    buynow.id, buynow.item
  end

  Index 'all_categories' do
    Hash    categories.dummy
    Ordered categories.id
    Path    categories.id
  end

  Index 'all_regions' do
    Hash    regions.dummy
    Ordered regions.id
    Path    regions.id
  end

  Index 'bids_by_item' do
    Hash    items.id
    Ordered bids.id
    Path    items.id, items.bids
  end

  Index 'items_by_category' do
    Hash    categories.id
    Ordered items.end_date, items.id
    Path    categories.id, categories.items
  end

  Index 'items_by_region' do
    Hash    regions.id
    Ordered categories.id, items.end_date, items.id, users.id
    Path    regions.id, regions.users, users.items_sold, items.category
  end

  Index 'comments_by_user' do
    Hash    users.id
    Ordered comments.id
    Path    users.id, users.comments_received
  end

  Index 'user_items_sold' do
    Hash    users.id
    Ordered items.end_date, items.id
    Path    users.id, users.items_sold
  end

  Index 'buynow_by_user' do
    Hash    users.id
    Ordered buynow.date, buynow.id
    Path    users.id, users.bought_now
  end

  Index 'bids_by_user' do
    Hash    users.id
    Ordered bids.date, bids.id
    Path    users.id, users.bids
  end
end
