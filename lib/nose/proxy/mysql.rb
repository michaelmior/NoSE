# frozen_string_literal: true

require 'mysql'

module NoSE
  module Proxy
    # A proxy which speaks the MySQL protocol and executes queries
    class MysqlProxy < ProxyBase
      def initialize(*args)
        super

        # Initialize a hash for the state of sockets
        @state = {}
      end

      # Authenticate the client and process queries
      def handle_connection(socket)
        return authenticate socket if @state[socket].nil?

        # Retrieve the saved state of the socket
        protocol = @state[socket]

        begin
          protocol.process_command(&method(:process_query))
        rescue ::Mysql::ClientError::ServerGoneError
          # Ensure the socket is closed and remove the state
          remove_connection socket
          return false
        end

        # Keep this socket around
        true
      end

      # Remove the state of the socket
      def remove_connection(socket)
        socket.close
        @state.delete socket
      end

      private

      # Auth the client and prepare for query processsing
      # @return [Boolean]
      def authenticate(socket)
        protocol = ::Mysql::ServerProtocol.new socket

        # Try to authenticate
        begin
          protocol.authenticate
        rescue
          remove_connection socket
          return false
        end

        @state[socket] = protocol

        true
      end

      # Execute the query on the backend and return the result
      def process_query(protocol, query)
        begin
          @logger.debug { "Got query #{query}" }
          result = query_result query
          @logger.debug "Executed query with #{result.size} results"
        rescue ParseFailed => exc
          protocol.error ::Mysql::ServerError::ER_PARSE_ERROR, exc.message
        rescue Backend::PlanNotFound => exc
          protocol.error ::Mysql::ServerError::ER_UNKNOWN_STMT_HANDLER,
                         exc.message
        end

        result
      end

      private

      # Get the result of the query from the backend
      def query_result(query)
        query = Statement.parse query, @result.workload.model
        @backend.query(query).lazy.map do |row|
          Hash[query.select.map { |field| [field.name, row[field.id]] }]
        end
      end
    end
  end

  # Extend the client library with necessary server code
  class ::Mysql
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
        AuthenticationPacket.parse read # TODO: Check auth
        write ResultPacket.serialize 0
      end

      # Send an error message with the given number and text
      def error(errno, message)
        write ErrorPacket.serialize errno, message
      end

      # Process a single incoming command
      def process_command(&block)
        reset
        pkt = read
        command = pkt.utiny

        case command
        when COM_QUIT
          # Stop processing because the client left
          return
        when COM_QUERY
          process_query pkt.to_s, &block
        when COM_PING
          write ResultPacket.serialize 0
        else
          # Return error for invalid commands
          protocol.error ::Mysql::ServerError::ER_NOT_SUPPORTED_YET,
                         'Command not supported'
        end
      end

      private

      # Handle an individual query
      def process_query(query)
        # Execute the query on the backend
        result = yield self, query
        return if result.nil?

        # Return the list of fields in the result
        field_names = result.any? ? result.peek.keys : []
        write_fields result, field_names
        write_rows result, field_names
      end

      # Write the list of fields for the resulting rows
      def write_fields(result, field_names)
        write ResultPacket.serialize field_names.count
        field_names.each do |field_name|
          type, = Protocol.value2net result.first[field_name]

          write FieldPacket.serialize '', '', '', field_name, '', 1, type,
                                      Field::NOT_NULL_FLAG, 0, ''
        end
        write EOFPacket.serialize
      end

      # Write a packet for each row in the results
      def write_rows(result, field_names)
        result.each do |row|
          values = field_names.map { |field_name| row[field_name] }
          write(values.map do |value|
            Protocol.value2net(value.to_s).last
          end.inject('', &:+))
        end
        write EOFPacket.serialize
      end
    end

    # Add serialization of the initial packet
    class InitialPacket
      # Serialize the initial server hello
      # @return [String]
      def self.serialize
        [
          ::Mysql::Protocol::VERSION,
          'nose',
          0,
          'AAAAAAAA',
          0,
          CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION,
          33, # utf8_general_ci
          SERVER_STATUS_AUTOCOMMIT,
          'AAAAAAAAAAAA'
        ].pack('CZ*Va8CvCvx13Z*')
      end
    end

    # Add serialization of result packets
    class ResultPacket
      # Serialize a simple OK response
      # rubocop:disable Metrics/ParameterLists
      # @return [String]
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
      # rubocop:enable Metrics/ParameterLists
    end

    # Add serialization of field packets
    class FieldPacket
      # Serialize all the data for a field
      # rubocop:disable Metrics/ParameterLists
      # @return [String]
      def self.serialize(db, table, org_table, name, org_name, length, type,
                         flags, decimals, default)
        Packet.lcs('def') + # catalog
          Packet.lcs(db) +
          Packet.lcs(table) +
          Packet.lcs(org_table) +
          Packet.lcs(name) +
          Packet.lcs(org_name) +
          [
            0x0c,
            33, # utf8_general_ci
            length,
            type,
            flags,
            decimals,
            0
          ].pack('CvVCvCv') + Packet.lcs(default)
      end
      # rubocop:enable Metrics/ParameterLists
    end

    # Add parsing of auth packets
    class AuthenticationPacket
      # Parse the incoming authentication packet
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

    # Simple EOF packet
    class EOFPacket
      # Static string to indicate EOF
      # @return [String]
      def self.serialize
        "\xfe\x00\x00\x00\x00"
      end
    end

    # Serialize an error message
    class ErrorPacket
      # Generate a packet with a given error number and message
      # @return [String]
      def self.serialize(errno, message)
        [
          0xff,
          errno,
          '#',
          @sqlstate,
          message
        ].pack('Cvaa5a*')
      end
    end
  end
end
