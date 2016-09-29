# frozen_string_literal: true

NoSE::Plans::ExecutionPlans.new do
  Schema 'rubis_baseline'

  DefaultMix :browsing

  Group 'BrowseCategories', browsing: 4.44,
                            bidding: 7.65,
                            write_medium: 7.65,
                            write_heavy: 7.65 do
    Plan 'Authentication' do
      Select users.password
      Param  users.id, :==
      Lookup 'users', [users.id, :==]
    end

    Plan 'Categories' do
      Select categories['*']
      Param  categories.dummy, :==, 1
      Lookup 'all_categories', [categories.dummy, :==]
      Lookup 'categories', [categories.id, :==]
    end
  end

  Group 'ViewBidHistory', browsing: 2.38,
                          bidding: 1.54,
                          write_medium: 1.54,
                          write_heavy: 1.54 do
    Plan 'ItemName' do
      Select items.name
      Param  items.id, :==
      Lookup 'items', [items.id, :==]
    end

    Plan 'Bids' do
      Select bids['*'], users.id, users.nickname
      Param  items.id, :==
      Lookup 'bids_by_item', [items.id, :==]
      Lookup 'bids', [bids.id, :==]
      Lookup 'users', [users.id, :==]
    end
  end

  Group 'ViewItem', browsing: 22.95,
                    bidding: 14.17,
                    write_medium: 14.17,
                    write_heavy: 14.17 do
    Plan 'ItemData' do
      Select items['*']
      Param  items.id, :==
      Lookup 'items', [items.id, :==]
    end

    Plan 'Bids' do
      Select bids['*']
      Param  items.id, :==
      Lookup 'bids_by_item', [items.id, :==]
      Lookup 'bids', [bids.id, :==]
    end
  end

  Group 'SearchItemsByCategory', browsing: 27.77,
                                 bidding: 15.94,
                                 write_medium: 15.94,
                                 write_heavy: 15.94 do
    Plan 'ItemList' do
      Select items['*']
      Param  categories.id, :==
      Param  items.end_date, :>=
      Lookup 'items_by_category',
             [categories.id, :==],
             [items.end_date, :>=], limit: 25
      Lookup 'items', [items.id, :==]
    end
  end

  Group 'SearchItemsByRegion', browsing: 8.26,
                               bidding: 6.34,
                               write_medium: 6.34,
                               write_heavy: 6.34 do
    Plan 'ItemList' do
      Select items['*']
      Param  regions.id, :==
      Param  categories.id, :==
      Param  items.end_date, :>=
      Lookup 'items_by_region',
             [regions.id, :==],
             [categories.id, :==],
             [items.end_date, :>=], limit: 25
      Lookup 'items', [items.id, :==]
    end
  end

  Group 'BrowseRegions', browsing: 3.21,
                         bidding: 5.39,
                         write_medium: 5.39,
                         write_heavy: 5.39 do
    Plan 'Regions' do
      Select regions['*']
      Param  regions.dummy, :==, 1
      Lookup 'all_regions', [regions.dummy, :==]
      Lookup 'regions', [regions.id, :==]
    end
  end

  Group 'ViewUserInfo', browsing: 4.41,
                        bidding: 2.48,
                        write_medium: 2.48,
                        write_heavy: 2.48 do
    Plan 'UserData' do
      Select users['*'], regions.id, regions.name
      Param  users.id, :==
      Lookup 'users', [users.id, :==]
      Lookup 'regions', [regions.id, :==]
    end

    Plan 'CommentsReceived' do
      Select comments['*']
      Param  users.id, :==
      Lookup 'comments_by_user', [users.id, :==]
      Lookup 'comments', [comments.id, :==]
    end
  end

  Group 'RegisterItem', bidding: 0.53,
                        write_medium: 0.53 * 10,
                        write_heavy: 0.53 * 100 do
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
      Insert 'items'
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

    Plan 'AddToRegion' do
      Support do
        Plan 'GetRegion' do
          Select regions.id
          Param  users.id, :==
          Lookup 'users', [users.id, :==]
        end
      end

      Param  users.id, :==
      Param  items.id, :==
      Param  items.end_date, :==
      Param  regions.id, :==
      Param  categories.id, :==
      Insert 'items_by_region'
    end
  end

  Group 'RegisterUser', bidding: 1.07,
                        write_medium: 1.07 * 10,
                        write_heavy: 1.07 * 100 do
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
      Insert 'users'
    end

    Plan 'AddToRegion' do
      Param  users.id, :==
      Param  users.nickname, :==
      Param  regions.id, :==
      Insert 'users_by_region'
    end
  end

  Group 'BuyNow', bidding: 1.16,
                  write_medium: 1.16,
                  write_heavy: 1.16 do
    Plan 'Authentication' do
      Select users.password
      Param  users.id, :==
      Lookup 'users', [users.id, :==]
    end

    Plan 'ItemData' do
      Select items['*']
      Param  items.id, :==
      Lookup 'items', [items.id, :==]
    end
  end

  Group 'StoreBuyNow', bidding: 1.10,
                       write_medium: 1.10 * 10,
                       write_heavy: 1.10 * 100 do
    Plan 'ReduceQuantity' do
      Support do
        Plan 'OldQuantity' do
          Select items.quantity
          Param items.id, :==
          Lookup 'items', [items.id, :==]
        end
      end

      Param  items.id, :==
      Insert 'items', items.id, items.quantity
    end

    Plan 'AddBuyNow' do
      Param  buynow.id, :==
      Param  buynow.qty, :==
      Param  buynow.date, :==
      Param  items.id, :==
      Insert 'buynow'
    end

    Plan 'AddToBought' do
      Param users.id, :==
      Param buynow.id, :==
      Param buynow.date, :==
      Insert 'buynow_by_user'
    end
  end

  Group 'PutBid', bidding: 5.40, write_medium: 5.40, write_heavy: 5.40 do
    Plan 'Authentication' do
      Select users.password
      Param  users.id, :==
      Lookup 'users', [users.id, :==]
    end

    Plan 'ItemData' do
      Select items['*']
      Param  items.id, :==
      Lookup 'items', [items.id, :==]
    end

    Plan 'Bids' do
      Select bids['*']
      Param  items.id, :==
      Lookup 'bids_by_item', [items.id, :==]
      Lookup 'bids', [bids.id, :==]
    end
  end

  Group 'StoreBid', bidding: 3.74,
                    write_medium: 3.74 * 10,
                    write_heavy: 3.74 * 100 do
    Plan 'CheckMaxBid' do
      Select items.nb_of_bids, items.max_bid
      Param  items.id, :==
      Lookup 'items', [items.id, :==]
    end

    Plan 'AddBid' do
      Support do
        Plan 'GetMaxBid' do
          Select items.max_bid
          Param  items.id, :==
          Lookup 'items', [items.id, :==]
        end
      end

      Param  items.id, :==
      Param  items.max_bid, :==
      Insert 'items', items.id, items.max_bid
    end

    Plan 'AddToBids' do
      Param  bids.id, :==
      Param  bids.qty, :==
      Param  bids.bid, :==
      Param  bids.date, :==
      Param  users.id, :==
      Insert 'bids'
    end

    Plan 'AddToItemBids' do
      Param  items.id, :==
      Param  bids.id, :==
      Insert 'bids_by_item'
    end

    Plan 'AddToUserBids' do
      Param  users.id, :==
      Param  bids.id, :==
      Param  bids.date, :==
      Insert 'bids_by_user'
    end
  end

  Group 'PutComment', bidding: 0.46,
                      write_medium: 0.46,
                      write_heavy: 0.46 do
    Plan 'Authentication' do
      Select users.password
      Param  users.id, :==
      Lookup 'users', [users.id, :==]
    end

    Plan 'ItemData' do
      Select items['*']
      Param  items.id, :==
      Lookup 'items', [items.id, :==]
    end

    Plan 'UserData' do
      Select users['*']
      Param  users.id, :==
      Lookup 'users', [users.id, :==]
    end
  end

  Group 'StoreComment', bidding: 0.45,
                        write_medium: 0.45 * 10,
                        write_heavy: 0.45 * 100 do
    Plan 'UpdateRating' do
      Support do
        Plan 'GetRating' do
          Select users.rating, regions.id
          Param  users.id, :==
          Lookup 'users', [users.id, :==]
        end
      end

      Param  users.id, :==
      Param  users.rating, :==
      Insert 'users', users.id, regions.id, users.rating
    end

    Plan 'InsertComment' do
      Param  comments.id, :==
      Param  comments.rating, :==
      Param  comments.date, :==
      Param  comments.comment, :==
      Insert 'comments'
    end

    Plan 'AddToUserComments' do
      Param  users.id, :==
      Param  comments.id, :==
      Insert 'comments_by_user'
    end
  end

  Group 'AboutMe', bidding: 1.71,
                   write_medium: 1.71,
                   write_heavy: 1.71 do
    Plan 'UserData' do
      Select users['*']
      Param  users.id, :==
      Lookup 'users', [users.id, :==]
    end

    Plan 'CommentsReceived' do
      Select comments['*']
      Param  users.id, :==
      Lookup 'comments_by_user', [users.id, :==]
      Lookup 'comments', [comments.id, :==]
    end

    Plan 'BuyNow' do
      Select  items['*']
      Param   users.id, :==
      Param   buynow.date, :>=
      Lookup 'buynow_by_user', [users.id, :==], [buynow.date, :>=]
      Lookup 'buynow', [buynow.id, :==]
      Lookup 'items', [items.id, :==]
    end

    Plan 'ItemsSold' do
      Select  items['*']
      Param   users.id, :==
      Param   items.end_date, :>=
      Lookup 'user_items_sold', [users.id, :==], [items.end_date, :>=]
      Lookup 'items', [items.id, :==]
    end

    Plan 'ItemsBid' do
      Select items['*'], bids.id
      Param  users.id, :==
      Param  bids.date, :>=
      Lookup 'bids_by_user', [users.id, :==], [bids.date, :>=]
      Lookup 'bids', [bids.id, :==]
      Lookup 'items', [items.id, :==]
    end
  end
end
