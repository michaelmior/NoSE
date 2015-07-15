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
        backend = get_backend(options, result)

        # Load the history file
        history_file = File.expand_path '~/.nose_history'
        if Object.const_defined? 'Readline'
          begin
            File.foreach(history_file) do |line|
              Readline::HISTORY.push line.chomp
            end
          rescue Errno::ENOENT
            nil
          end
        end

        loop do
          begin
            line = read_line
          rescue Interrupt
            line = nil
          end
          break if line.nil?

          next if line.empty?

          execute_statement line, result, backend
        end

        # Save the history file
        if Object.const_defined? 'Readline'
          File.open(history_file, 'w') do |f|
            Readline::HISTORY.each do |line|
              f.puts line
            end
          end
        end
      end

      private

      # Get the next inputted line in the REPL
      def read_line
        prefix = '>> '

        if Object.const_defined? 'Readline'
          line = Readline.readline prefix
          return if line.nil?

          Readline::HISTORY.push line unless line.chomp.empty?
        else
          print prefix
          line = gets
        end

        line.chomp
      end

      # Parse a statement from a given string of text
      def parse_statement(text, workload)
        begin
          statement = Statement.parse text, workload.model
        rescue ParseFailed => e
          puts '! ' + e.message
          statement = nil
        end

        statement
      end

      # Try to execute a statement read from the REPL
      def execute_statement(line, result, backend)
        # Parse the statement
        statement = parse_statement line, result.workload
        return if statement.nil?

        begin
          start_time = Time.now

          if statement.is_a? Query
            results = backend.query statement
          else
            backend.update statement
            results = []
          end

          elapsed = Time.now - start_time
        rescue NotImplementedError, Backend::PlanNotFound => e
          puts '! ' + e.message
        else
          Formatador.display_compact_table results unless results.empty?
          puts format('(%d rows in %.2fs)', results.length, elapsed)
        end
      end
    end
  end
end
