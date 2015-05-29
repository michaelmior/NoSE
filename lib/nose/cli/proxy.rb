module NoSE
  module CLI
    # Add a command to run a proxy server capable of executing queries
    class NoSECLI < Thor
      desc 'proxy PLAN_FILE', 'start a proxy with the given PLAN_FILE'
      def proxy(plan_file)
        result = load_results plan_file
        config = load_config
        backend = get_backend(config, result)

        # Create a new instance of the proxy class
        proxy_class = get_class 'proxy', config
        proxy = proxy_class.new config[:proxy], result, backend

        # Start the proxy server
        trap 'INT' do
          proxy.stop
        end
        proxy.start
      end
    end
  end
end
