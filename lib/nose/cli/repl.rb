require 'formatador'

module NoSE::CLI
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

      begin
        require 'readline'
        line = Readline.readline prefix
        return if line.nil?

        Readline::HISTORY.push line
      rescue LoadError
        print prefix
        line = gets
      end

      line
    end

    # Try to execute a query read from the REPL
    def execute_query(line, result, backend)
      begin
        query = NoSE::Statement.parse line, result.workload
      rescue NoSE::ParseFailed => e
        puts '! ' + e.message
        return
      end

      begin
        start_time = Time.now
        results = backend.query(query)
        elapsed = Time.now - start_time
      rescue NotImplementedError => e
        puts '! ' + e.message
      else
        Formatador.display_compact_table results unless results.empty?
        puts '(%d rows in %.2fs)' % [results.length, elapsed]
      end
    end
  end
end
