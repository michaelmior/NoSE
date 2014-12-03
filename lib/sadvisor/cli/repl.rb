require 'formatador'

module Sadvisor
  class SadvisorCLI < Thor
    desc 'repl PLAN_FILE', 'start the REPL with the given PLAN_FILE'
    def repl(plan_file)
      result = load_results plan_file
      config = load_config
      backend = get_backend(config, result)

      loop do
        line = get_line
        break if line.nil?
        line.chomp!
        next if line.empty?
        query = Statement.new line, result.workload

        # Execute the query
        begin
          result = backend.query(query)
        rescue NotImplementedError => e
          puts '! ' + e.message
        else
          Formatador.display_compact_table result
        end

      end
    end

    private

    # Get the next inputted line in the REPL
    def get_line
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
  end
end
