# frozen_string_literal: true

NoSE::Plans::ExecutionPlans.new do
  Schema 'ebay'

  Group 'GetUser' do
    Plan 'GetUser' do
      Select users['*']
      Param users.UserID, :==
      Lookup 'users_by_id', [users.UserID, :==]
    end
  end

  Group 'GetItem' do
    Plan 'GetItem' do
      Select items['*']
      Param items.ItemID, :==
      Lookup 'items_by_id', [items.ItemID, :==]
    end
  end

  Group 'GetUserLikes' do
    Plan 'GetUserLikes' do
      Select items['*']
      Param users.UserID, :==
      Lookup 'likes_by_user', [users.UserID, :==]
      Lookup 'items_by_id', [items.ItemID, :==]
    end
  end

  Group 'GetItemLikes' do
    Plan 'GetItemLikes' do
      Select users['*']
      Param items.ItemID, :==
      Lookup 'likes_by_item', [items.ItemID, :==]
      Lookup 'users_by_id', [users.UserID, :==]
    end
  end

  Group 'AddLike' do
    Plan 'AddItemLike' do
      Param items.ItemID, :==
      Param likes.LikeID, :==
      Param likes.LikedAt, :==
      Param users.UserID, :==
      Insert 'likes_by_item'
    end

    Plan 'AddUserLike' do
      Param users.UserID, :==
      Param likes.LikeID, :==
      Param likes.LikedAt, :==
      Param items.ItemID, :==
      Insert 'likes_by_user'
    end
  end

  Group 'AddUser' do
    Plan 'AddUser' do
      Param users.UserID, :==
      Param users.Name, :==
      Param users.Email, :==
      Insert 'users_by_id'
    end
  end

  Group 'AddItem' do
    Plan 'AddItem' do
      Param items.ItemID, :==
      Param items.Title, :==
      Param items.Desc, :==
      Insert 'items_by_id'
    end
  end

  Group 'UpdateItemTitle' do
    Plan 'UpdateItemTitle' do
      Param items.ItemID, :==
      Param items.Title, :==
      Insert 'items_by_id', items.ItemID, items.Title
    end
  end
end
