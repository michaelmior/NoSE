# rubocop:disable SingleSpaceBeforeFirstArg

NoSE::ExecutionPlans.new do
  Schema 'rubis_expert'

  Group 'BrowseCategories', browsing: 4.44 + 3.21, bidding: 7.65 + 5.39 do
    Plan 'Authentication' do
      Select users.password
      Param  users.id, :==
      Lookup 'user_data', [users.id, :==]
    end

    Plan 'Categories' do
      Select categories['*']
      Param  categories.dummy, :==, 1
      Lookup 'categories', [categories.dummy, :==]
    end
  end

  Group 'ViewBidHistory', browsing: 2.38, bidding: 1.54 do
    Plan 'ItemName' do
      Select items.name
      Param  items.id, :==
      Lookup 'items_data', [items.id, :==]
    end

    Plan 'Bids' do
      Select bids['*']
      Param  items.id, :==
      Lookup 'item_bids', [items.id, :==]
    end
  end

  Group 'ViewItem', browsing: 22.95, bidding: 14.17 do
    Plan 'ItemData' do
      Select items['*']
      Param  items.id, :==
      Lookup 'items_data', [items.id, :==]
    end
  end

  Group 'SearchItemsByCategory', browsing: 27.77 + 8.26, bidding: 15.94 + 6.34 do
    Plan 'ItemList' do
      Select items['*']
      Param  categories.id, :==
      Param  items.end_date, :>=
      Lookup 'items_by_category',
             [categories.id, :==],
             [items.end_date, :>=], limit: 25
      Lookup 'items_data', [items.id, :==]
    end
  end

  # XXX Not currently supported
  # # SearchItemsByRegion
  # # BrowseRegions

  Group 'ViewUserInfo', browsing: 4.41, bidding: 2.48 do
    Plan 'UserData' do
      Select users['*'], regions.name
      Param  users.id, :==
      Lookup 'user_data', [users.id, :==]
    end

    Plan 'CommentsReceived' do
      Select comments['*']
      Param  users.id, :==
      Lookup 'user_comments_received', [users.id, :==]
      Lookup 'commenter', [comments.id, :==]
    end
  end

  Group 'RegisterItem', bidding: 0.53 do
  end

  Group 'RegisterUser', bidding: 1.07 do
  end

  Group 'BuyNow', bidding: 1.16 do
    Plan 'Authentication' do
      Select users.password
      Param  users.id, :==
      Lookup 'user_data', [users.id, :==]
    end
  end

  Group 'StoreBuyNow', bidding: 1.10 do
  end

  Group 'PutBid', bidding: 5.40 do
    Plan 'Authentication' do
      Select users.password
      Param  users.id, :==
      Lookup 'user_data', [users.id, :==]
    end

    Plan 'ItemData' do
      Select items['*']
      Param  items.id, :==
      Lookup 'items_data', [items.id, :==]
    end
  end

  Group 'StoreBid', bidding: 3.74 do
  end

  Group 'PutComment', bidding: 0.46 do
    Plan 'Authentication' do
      Select users.password
      Param  users.id, :==
      Lookup 'user_data', [users.id, :==]
    end

    Plan 'ItemData' do
      Select items['*']
      Param  items.id, :==
      Lookup 'items_data', [items.id, :==]
    end

    Plan 'UserData' do
      Select users['*']
      Param  users.id, :==
      Lookup 'user_data', [users.id, :==]
    end
  end

  Group 'StoreComment', bidding: 0.45 do
  end

  Group 'AboutMe', bidding: 1.71 do
    Plan 'UserData' do
      Select users['*']
      Param  users.id, :==
      Lookup 'user_data', [users.id, :==]
    end

    Plan 'CommentsReceived' do
      Select comments['*']
      Param  users.id, :==
      Lookup 'user_comments_received', [users.id, :==]
      Lookup 'commenter', [comments.id, :==]
    end

    Plan 'BuyNow' do
      Select  items['*']
      Param   users.id, :==
      Param   buynow.date, :>=
      Lookup 'user_buynow', [users.id, :==], [buynow.date, :>=]
      Lookup 'items_data', [items.id, :==]
    end

    Plan 'ItemsSold' do
      Select  items['*']
      Param   users.id, :==
      Param   items.end_date, :>=
      Lookup 'user_items_sold', [users.id, :==], [items.end_date, :>=]
      Lookup 'items_data', [items.id, :==]
    end

    Plan 'ItemsBid' do
      Select items['*']
      Param  users.id, :==
      Param  bids.date, :>=
      Lookup 'user_items_bid_on', [users.id, :==], [bids.date, :>=]
      Lookup 'items_data', [items.id, :==]
    end
  end
end

# rubocop:enable SingleSpaceBeforeFirstArg
