# frozen_string_literal: true
# rubocop:disable all

NoSE::Model.new do
  # Define entities along with the size and cardinality of their fields
  # as well as an estimated number of each entity

  (Entity 'Player' do
    ID      'PlayerID'
    String  'PlayerName'
    Integer 'PlayerFlags'
    Boolean 'IsAdmin'
  end) * 100_000

  (Entity 'Session' do
    ID      'SessionID'
    Date    'TimeStarted'
    Date    'TimeEnded'
  end) * 100_000

  (Entity 'PlayerState' do
    ID      'StateID'
    Float   'PosX'
    Float   'PosY'
    Float   'PosZ'
    Date    'ClientTimestamp'
    Date    'ServerTimestamp'
  end) * 4_000_000

  (Entity 'Server' do
    ID      'ServerID'
    String  'ServerIP'
    String  'ServerName'
  end) * 5_000

  HasOne 'player',    'sessions',
         'Session' => 'Player'

  HasOne 'server',    'sessions',
         'Session' => 'Server'

  HasOne 'session',       'states',
         'PlayerState' => 'Session'
end
