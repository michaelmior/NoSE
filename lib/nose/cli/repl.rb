# frozen_string_literal: true

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

      long_desc <<-LONGDESC
        `nose repl` starts a terminal interface which loads execution plans
        from the given file. It then loops continuously and accepts statements
        from the workload and executes plans against the database. The result
        of any query will be printed to the terminal.
      LONGDESC

      def repl(plan_file)
        result = load_results plan_file
        backend = get_backend(options, result)

        load_history
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
      end

      private

      # Load the history file
      # @return [void]
      def load_history
        return unless Object.const_defined? 'Readline'

        history_file = File.expand_path '~/.nose_history'
        begin
          File.foreach(history_file) do |line|
            Readline::HISTORY.push line.chomp
          end
        rescue Errno::ENOENT
          nil
        end
      end

      # Save the history file
      # @return [void]
      def save_history
        return unless Object.const_defined? 'Readline'

        history_file = File.expand_path '~/.nose_history'
        File.open(history_file, 'w') do |f|
          Readline::HISTORY.each do |line|
            f.puts line
          end
        end
      end

      # Get the next inputted line in the REPL
      # @return [String]
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
      # @return [Statement]
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
      # @return [void]
      def execute_statement(line, result, backend)
        # Parse the statement
        statement = parse_statement line, result.workload
        return if statement.nil?

        begin
          results, elapsed = backend_execute_statement backend, statement
        rescue NotImplementedError, Backend::PlanNotFound => e
          puts '! ' + e.message
        else
          display_statement_result results, elapsed
        end
      end

      # Execute the statement on the provided backend and measure the runtime
      def backend_execute_statement(backend, statement)
        start_time = Time.now.utc

        if statement.is_a? Query
          results = backend.query statement
        else
          backend.update statement
          results = []
        end

        elapsed = Time.now.utc - start_time

        [results, elapsed]
      end

      # Show the results of executing a statement
      # @return [void]
      def display_statement_result(results, elapsed)
        Formatador.display_compact_table results unless results.empty?
        puts format('(%d rows in %.2fs)', results.length, elapsed)
      end
    end
  end
end
