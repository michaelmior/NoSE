require 'formatador'

begin
  require 'readline'
rescue LoadError
  nil
end

module NoSE
  module CLI
    # Add a command to run a REPL which evaluates queries
    class NoSECLI < Thor
      desc 'repl PLAN_FILE', 'start the REPL with the given PLAN_FILE'
      def repl(plan_file)
        result = load_results plan_file
        config = load_config
        backend = get_backend(config, result)

        loop do
          begin
            line = read_line
          rescue Interrupt
            line = nil
          end
          break if line.nil?

          line.chomp!
          next if line.empty?

          execute_query line, result, backend
        end
      end

      private

      # Get the next inputted line in the REPL
      def read_line
        prefix = '>> '

        if Object.const_defined? 'Readline'
          line = Readline.readline prefix
          return if line.nil?

          Readline::HISTORY.push line
        else
          print prefix
          line = gets
        end

        line
      end

      # Parse a query from a given string of text
      def parse_query(text, workload)
        begin
          query = Statement.parse text, workload
        rescue ParseFailed => e
          puts '! ' + e.message
          query = nil
        end

        query
      end

      # Try to execute a query read from the REPL
      def execute_query(line, result, backend)
        # Parse the query
        query = parse_query line, result.workload
        return if query.nil?

        begin
          start_time = Time.now
          results = backend.query query
          elapsed = Time.now - start_time
        rescue NotImplementedError => e
          puts '! ' + e.message
        else
          Formatador.display_compact_table results unless results.empty?
          puts format('(%d rows in %.2fs)', results.length, elapsed)
        end
      end
    end
  end
end
