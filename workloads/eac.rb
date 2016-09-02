# frozen_string_literal: true

NoSE::Workload.new do
  Model 'eac'

  # Server session exists
  Q 'SELECT Server.ServerID FROM Server WHERE ' \
    'Server.ServerID = ?', 3

  # Get sessions by GUID
  Q 'SELECT Session.SessionID FROM ' \
    'Session.player WHERE player.PlayerID = ?', 3

  # Get player session
  Q 'SELECT states.PosX, states.PosY, states.PosZ, ' \
    'states.ServerTimestamp FROM ' \
    'Server.sessions.states WHERE Server.ServerID = ? AND ' \
    'sessions.player.PlayerID = ? ORDER BY states.ServerTimestamp', 6

  # Get new data
  Q 'SELECT states.PosX, states.PosY, states.PosZ, ' \
    'states.ServerTimestamp, sessions.player.PlayerID FROM ' \
    'Server.sessions.states WHERE sessions.player.IsAdmin = 0 AND ' \
    'Server.ServerID = ? AND states.ServerTimestamp > ? AND ' \
    'states.ServerTimestamp <= ? ORDER BY states.ServerTimestamp', 6

  # Get server information
  Q 'SELECT Server.ServerName, Server.ServerIP FROM ' \
    'Server WHERE Server.ServerID = ?', 2

  # Add new player
  Q 'INSERT INTO Player SET PlayerID=?, PlayerName=?, PlayerFlags=?, ' \
    'IsAdmin=?', 4

  # Record new state
  Q 'INSERT INTO PlayerState SET StateID=?, PosX=?, PosY=?, PosZ=?, ' \
    'ClientTimestamp=?, ServerTimestamp=? AND CONNECT TO session(?)', 71

  Q 'INSERT INTO Session SET SessionID=?, TimeStarted=?, TimeEnded=? ' \
    'AND CONNECT TO server(?), player(?)', 4

  Q 'INSERT INTO Server SET ServerID=?, ServerIP=?, ' \
    'ServerName=?', 1
end
