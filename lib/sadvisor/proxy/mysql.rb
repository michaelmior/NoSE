require 'mysql'

module Sadvisor
  class MySQLProxy < Proxy
    def handle_connection(socket)
      # Auth the client and begin query processsing
      protocol = Mysql::ServerProtocol.new socket
      protocol.authenticate
      protocol.process_queries do |query|
        begin
          @logger.debug { "Got query #{query}" }

          query = Statement.new query, @result.workload
          result = @backend.query(query)

          @logger.debug "Executed query with #{result.size} results"
        rescue
          # TODO: Proper error handling
          result = []
        end

        result
      end
    end
  end
end

class Mysql
  # Simple class which doesn't do connection setup
  class ServerProtocol < Protocol
    def initialize(socket)
      # We need a much simpler initialization than the default class
      @sock = socket
    end

    # Perform authentication
    def authenticate
      reset
      write InitialPacket.serialize
      AuthenticationPacket.parse read  # TODO: Check auth
      write ResultPacket.serialize 0
    end

    # Loop and process incoming queries
    def process_queries(&block)
      loop do
        reset
        pkt = read
        command = pkt.utiny

        case command
        when COM_QUIT
          # Stop processing because the client left
          break
        when COM_QUERY
          process_query pkt.to_s, &block
        when COM_PING
          write ResultPacket.serialize 0
        else
          # TODO: Return error for invalid commands
        end
      end
    end

    private

    # Handle an individual query
    def process_query(query, &block)
      # Execute the query on the backend
      result = block.call query

      # Return the list of fields in the result
      field_names = result.size > 0 ? result.first.keys : []
      write ResultPacket.serialize field_names.count
      field_names.each do |field_name|
        # TODO: Use proper types
        # type, _ = Protocol.value2net result.first[field_name]

        write FieldPacket.serialize '', '', '', field_name, '', 1,
          Field::TYPE_VAR_STRING, Field::NOT_NULL_FLAG, 0, ''
        end
      write "\xFE\x00\x00\x00\x00"

      result.each do |row|
        values = field_names.map { |field_name| row[field_name] }
        write(values.map do |value|
          Protocol.value2net(value.to_s).last
        end.inject('', &:+))
      end
      write "\xFE\x00\x00\x00\x00"
    end
  end

  # Add serialization of the initial packet
  class InitialPacket
    def self.serialize
      [
        Mysql::Protocol::VERSION,
        'sadvisor',
        0,
        'AAAAAAAA',
        0,
        CLIENT_PROTOCOL_41,
        33,  # utf8_general_ci
        SERVER_STATUS_AUTOCOMMIT,
        'AAAAAAAAAAAA'
      ].pack('CZ*Va8CvCvx13Z*')
    end
  end

  # Add serialization of result packets
  class ResultPacket
    def self.serialize(field_count, affected_rows = 0, insert_id = 0,
                       server_status = 0, warning_count = 0, message = '')
      return Packet.lcb(field_count) unless field_count.zero?

      Packet.lcb(field_count) +
      Packet.lcb(affected_rows) +
      Packet.lcb(insert_id) +
      [
        server_status,
        warning_count
      ].pack('vv') +
      Packet.lcs(message)
    end
  end

  # Add serialization of field packets
  class FieldPacket
    def self.serialize(db, table, org_table, name, org_name, length, type,
                       flags, decimals, default)
        Packet.lcs('def') +  # catalog
        Packet.lcs(db) +
        Packet.lcs(table) +
        Packet.lcs(org_table) +
        Packet.lcs(name) +
        Packet.lcs(org_name) +
        [
          0x0c,
          33,  # utf8_general_ci
          length,
          type,
          flags,
          decimals,
          0
        ].pack('CvVCvCv') + Packet.lcs(default)
    end
  end

  # Add parsing of auth packets
  class AuthenticationPacket
    def self.parse(_pkt)
      # XXX: Unneeded for now since we don't handle auth
      # client_flags = pkt.ulong
      # max_packet_size = pkt.ulong
      # charset_number = pkt.lcb
      # f1 = pkt.read(23)
      # username = pkt.string
      # scrambled_password = pkt.lcs
      # databasename = pkt.string
    end
  end
end
