module Sadvisor
  # A proxy server to interpret our query language and implement query plans
  class Proxy
    def initialize(config, result, backend)
      @result = result
      @backend = backend
      @config = config
    end

    def start
      return unless @server.nil?

      @server = TCPServer.new('127.0.0.1', @config[:port])
      loop do
        p @server
        handle_connection @server.accept
      end
    end

    def handle_connection(_socket)
    end

    def stop
      p 'STOP!'
      @server.close if @server
    end
  end
end
