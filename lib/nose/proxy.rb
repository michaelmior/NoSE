# frozen_string_literal: true

module NoSE
  # Query processing proxies to transparently execute queries against a backend
  module Proxy
    # A proxy server to interpret our query language and implement query plans
    class ProxyBase
      attr_reader :logger
      def initialize(config, result, backend)
        @logger = Logging.logger['nose::proxy']

        @result = result
        @backend = backend
        @config = config

        @continue = true
      end

      # Start the proxy server
      # @return [void]
      def start
        @logger.info "Starting server on port #{@config[:port]}"

        server_socket = TCPServer.new('127.0.0.1', @config[:port])
        server_socket.listen(100)

        @read_sockets = [server_socket]
        @write_sockets = []
        loop do
          break unless @continue && select_connection(server_socket)
        end
      end

      # @abstract Subclasses should process a new connection
      #           on the given socket
      # :nocov:
      # @return [void]
      def handle_connection(_socket)
        fail NotImplementedError
      end
      # :nocov:

      # @abstract Subclasses should dispose of state associated with the socket
      # :nocov:
      # @return [void]
      def remove_connection(_socket)
        fail NotImplementedError
      end
      # :nocov:

      # Stop accepting connections
      # @return [void]
      def stop
        @continue = false
      end

      private

      # Select sockets which are available to be processed
      # @return [void]
      def select_connection(server_socket)
        read, write, error = IO.select @read_sockets, @write_sockets,
                                       @read_sockets + @write_sockets, 5
        return true if read.nil?

        # Check if we have a new incoming connection
        if read.include? server_socket
          accept_connection server_socket
          read.delete server_socket
        elsif error.include? server_socket
          @logger.error 'Server socket died'
          return false
        end

        # Remove all sockets which have errors
        error.each { |socket| remove_connection socket }
        @read_sockets -= error
        @write_sockets -= error

        # Handle connections on each available socket
        process_connections read + write
      end

      # Accept the new connection
      # @return [void]
      def accept_connection(server_socket)
        client_socket, = server_socket.accept
        @read_sockets << client_socket
        @write_sockets << client_socket
      end

      # Process all pending connections
      # @return [void]
      def process_connections(sockets)
        sockets.each do |socket|
          @write_sockets.delete socket
          @read_sockets.delete socket unless handle_connection socket
        end
      end
    end
  end
end
