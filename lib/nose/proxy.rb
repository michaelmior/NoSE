module NoSE
  # A proxy server to interpret our query language and implement query plans
  class Proxy
    attr_reader :logger
    def initialize(config, result, backend)
      @logger = Logging.logger['nose::proxy']

      @result = result
      @backend = backend
      @config = config

      @continue = true
    end

    # Start the proxy server
    def start
      @logger.info "Starting server on port #{@config[:port]}"

      server_socket = TCPServer.new('127.0.0.1', @config[:port])
      server_socket.listen(100)

      sockets = [server_socket]
      loop do
        read, write, error = IO.select sockets, sockets, sockets, 5
        break unless @continue
        next if read.nil? || write.nil? || error.nil?

        # Check if we have a new incoming connection
        if read.include? server_socket
          socket, _ = server_socket.accept
          sockets << socket
          read.delete server_socket
        elsif error.include? server_socket
          @logger.error 'Server socket died'
          break
        end

        # Remove all sockets which have errors
        error.each { |socket| remove_connection socket }
        sockets -= error

        # Handle connections on each available socket
        (read + write).each do |socket|
          sockets.delete socket unless handle_connection socket
        end
      end
    end

    # @abstract Implemented by subclasses
    def handle_connection(_socket)
      raise NotImplementedError
    end

    # @abstract Implemented by subclasses
    def remove_connection(_socket)
      raise NotImplementedError
    end

    # Stop accepting connections
    def stop
      @continue = false
    end
  end
end
