require 'eventmachine'

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

      socket = TCPServer.new('127.0.0.1', @config[:port])
      socket.listen(100)
      EventMachine.epoll

      EventMachine::run do
        # Check when we need to shut down
        EM.add_periodic_timer(4) do
          unless @continue
            @logger.info 'Shutting down'
            EM.stop_event_loop
          end
        end

        # Start a new server
        EM.watch(socket, ProxyServer, self) do |conn|
          conn.proxy = self
          conn.notify_readable = true
        end
      end
    end

    # Implemented by subclasses
    def handle_connection(_socket)
      raise NotImplementedError
    end

    # Stop accepting connections
    def stop
      @continue = false
    end

    private

    # Simple connection subclass to pass things back up to the proxy
    class ProxyServer < EM::Connection
      attr_accessor :proxy

      def notify_readable
        while socket = @io.accept_nonblock
          @proxy.logger.debug 'Accepted new connection'
          @proxy.handle_connection socket
        end
      rescue Errno::EAGAIN, Errno::ECONNABORTED
      end

      def unbind
        detach
        @io.close
      end
    end
  end
end
