# frozen_string_literal: true

module NoSE
  module CLI
    # Add a command to run a proxy server capable of executing queries
    class NoSECLI < Thor
      desc 'proxy PLAN_FILE', 'start a proxy with the given PLAN_FILE'

      long_desc <<-LONGDESC
        `nose proxy` loads the schema and execution plans from the given file.
        It then starts the configured proxy server which is able to execute
        the given set of plans. Available proxy servers can be seen in the
        `lib/nose/proxy` subdirectory.
      LONGDESC

      def proxy(plan_file)
        result = load_results plan_file
        backend = get_backend(options, result)

        # Create a new instance of the proxy class
        proxy_class = get_class 'proxy', options
        proxy = proxy_class.new options[:proxy], result, backend

        # Start the proxy server
        trap 'INT' do
          proxy.stop
        end
        proxy.start
      end
    end
  end
end
