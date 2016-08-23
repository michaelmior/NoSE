# rubocop:disable all

NoSE::Workload.new do
  Model 'eac'

  # Server session exists
  Q 'SELECT Server.ServerID FROM Server WHERE ' \
    'Server.ServerID = ?', 1_000

  # Get sessions by GUID
  Q 'SELECT Session.SessionID, player.PlayerID FROM ' \
    'Session.player WHERE player.PlayerID = ?', 1_000

  # Get player session
  Q 'SELECT states.PosX, states.PosY, states.PosZ, states.Airborne, ' \
    'states.ServerTimestamp FROM ' \
    'Server.sessions.states WHERE Server.ServerID = ? AND ' \
    'sessions.player.PlayerID = ? ORDER BY states.ServerTimestamp', 10_000

  # Get new data
  Q 'SELECT states.PosX, states.PosY, states.PosZ, states.Airborne, ' \
    'states.ServerTimestamp, sessions.player.PlayerID FROM ' \
    'Server.sessions.states WHERE sessions.player.IsAdmin = 0 AND ' \
    'Server.ServerID = ? AND states.ServerTimestamp > ? AND ' \
    'states.ServerTimestamp <= ? ORDER BY states.ServerTimestamp', 10_000

  # Get server information
  Q 'SELECT Server.ServerName, Server.ServerIP FROM ' \
    'Server WHERE Server.ServerID = ?', 100

  # Add new player
  Q 'INSERT INTO Player SET PlayerID=?, PlayerName=?, PlayerFlags=?, ' \
    'IsAdmin=?', 10

  # Record new state
  Q 'INSERT INTO PlayerState SET StateID=?, PosX=?, PosY=?, PosZ=?, ' \
    'Airborne=?, ClientTimestamp=?, ServerTimestamp=? AND ' \
    'CONNECT TO session(?)', 100_000

  Q 'INSERT INTO Session SET SessionID=?, TimeStarted=?, TimeEnded=? ' \
    'AND CONNECT TO server(?), player(?)', 10

  Q 'INSERT INTO Server SET ServerID=?, ServerIP=?, ' \
    'ServerName=?', 1
end

# rubocop:enable all
