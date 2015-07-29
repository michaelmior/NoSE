# rubocop:disable SingleSpaceBeforeFirstArg

NoSE::ExecutionPlans.new do
  Schema 'rubis_expert'

  Group 'BrowseCategories', browsing: 4.44 + 3.21, bidding: 7.65 + 5.39 do
    Plan do
      Select users['*']
      Param  users.id, :==
      Lookup 'user_data', [users.id, :==]
    end

    Plan do
      Select categories['*']
      Param  categories.dummy, :==, 1
      Lookup 'categories', [categories.dummy, :==]
    end
  end

  Group 'ViewBidHistory', browsing: 2.38, bidding: 1.54 do
    Plan do
      Select items.name
      Param  items.id, :==
      Lookup 'items_data', [items.id, :==]
    end

    Plan do
      Select bids['*']
      Param  items.id, :==
      Lookup 'item_bids', [items.id, :==]
    end
  end

  Group 'ViewItem', browsing: 22.95, bidding: 14.17 do
    Plan do
      Select items['*']
      Param  items.id, :==
      Lookup 'items_data', [items.id, :==]
    end
  end

  Group 'SearchItemsByCategory', browsing: 27.77 + 8.26, bidding: 15.94 + 6.34 do
  end

  # XXX Not currently supported
  # # SearchItemsByRegion
  # # BrowseRegions

  Group 'ViewUserInfo', browsing: 4.41, bidding: 2.48 do
  end

  Group 'RegisterItem', bidding: 0.53 do
  end

  Group 'RegisterUser', bidding: 1.07 do
  end

  Group 'StoreBid', bidding: 3.74 do
  end

  Group 'StoreComment', bidding: 0.45 do
  end
end

# rubocop:enable SingleSpaceBeforeFirstArg
