# rubocop:disable SingleSpaceBeforeFirstArg

NoSE::Plans::ExecutionPlans.new do
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
      Lookup 'category_list', [categories.dummy, :==]
    end
  end

  Group 'ViewBidHistory', browsing: 2.38, bidding: 1.54 do
    Plan 'ItemName' do
      Select items.name
      Param  items.id, :==
      Lookup 'items_data', [items.id, :==]
    end

    Plan 'Bids' do
      Select bids['*'], users.id, users.nickname
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

    Plan 'Bids' do
      Select bids['*']
      Param  items.id, :==
      Lookup 'item_bids', [items.id, :==]
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

  Group 'RegisterItem', bidding: 0.53, write_heavy: 0.53 * 10 do
    Plan 'InsertItem' do
      Param  items.id, :==
      Param  items.name, :==
      Param  items.description, :==
      Param  items.initial_price, :==
      Param  items.quantity, :==
      Param  items.reserve_price, :==
      Param  items.buy_now, :==
      Param  items.nb_of_bids, :==
      Param  items.max_bid, :==
      Param  items.start_date, :==
      Param  items.end_date, :==
      Insert 'items_data'
    end

    Plan 'AddToSold' do
      Param  items.id, :==
      Param  items.end_date, :==
      Param  users.id, :==
      Insert 'user_items_sold'
    end

    Plan 'AddToCategory' do
      Param  items.id, :==
      Param  items.end_date, :==
      Param  categories.id, :==
      Insert 'items_by_category'
    end
  end

  Group 'RegisterUser', bidding: 1.07, write_heavy: 1.07 * 10 do
    Plan 'AddUser' do
      Support do
        Plan 'GetRegionName' do
          Select regions.name
          Param  regions.id, :==
          Lookup 'regions', [regions.id, :==]
        end
      end

      Param  users.id, :==
      Param  users.firstname, :==
      Param  users.lastname, :==
      Param  users.nickname, :==
      Param  users.password, :==
      Param  users.email, :==
      Param  users.rating, :==, 0
      Param  users.balance, :==, 0
      Param  users.creation_date, :==
      Param  regions.id, :==
      Param  regions.name, :==
      Insert 'user_data'
    end

    # XXX Not used since we don't implement browse regions
    # Plan 'AddToRegion' do
    #   Param  users.id, :==
    #   Param  users.nickname, :==
    #   Param  regions.id, :==
    #   Insert 'users_by_region'
    # end
  end

  Group 'BuyNow', bidding: 1.16 do
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

  Group 'StoreBuyNow', bidding: 1.10, write_heavy: 1.10 * 10 do
    Plan 'ReduceQuantity' do
      Support do
        Plan 'OldQuantity' do
          Select items.quantity
          Param items.id, :==
          Lookup 'items_data', [items.id, :==]
        end
      end

      Param  items.id, :==
      Insert 'items_data', items.id, items.quantity
    end

    Plan 'AddToBought' do
      Param users.id, :==
      Param items.id, :==
      Param buynow.id, :==
      Param buynow.qty, :==
      Param buynow.date, :==
      Insert 'user_buynow'
    end
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

    Plan 'Bids' do
      Select bids['*']
      Param  items.id, :==
      Lookup 'item_bids', [items.id, :==]
    end
  end

  Group 'StoreBid', bidding: 3.74, write_heavy: 3.74 * 10 do
    Plan 'AddBid' do
      Support do
        Plan 'GetMaxBid' do
          Select items.max_bid
          Param  items.id, :==
          Lookup 'item_bids', [items.id, :==], limit: 1
        end
      end

      Param  items.id, :==
      Param  users.id, :==
      Param  bids.id, :==
      Param  bids.qty, :==
      Param  bids.bid, :==
      Param  bids.date, :==
      Insert 'item_bids'
    end

    Plan 'AddToUserBids' do
      Param users.id, :==
      Param items.id, :==
      Param bids.id, :==
      Param bids.qty, :==
      Param bids.date, :==
      Insert 'user_items_bid_on'
    end
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

  Group 'StoreComment', bidding: 0.45, write_heavy: 0.45 * 10 do
    Plan 'UpdateRating' do
      Support do
        Plan 'GetRating' do
          Select users.rating, regions.id
          Param  users.id, :==
          Lookup 'user_data', [users.id, :==]
        end
      end

      Param  users.id, :==
      Insert 'user_data', users.id, users.rating, regions.id
    end

    Plan 'InsertComment' do
      Param  comments.id, :==
      Param  comments.rating, :==
      Param  comments.date, :==
      Param  comments.comment, :==
      Param  items.id, :==
      Param  users.id, :==
      Insert 'user_comments_received'
    end
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
