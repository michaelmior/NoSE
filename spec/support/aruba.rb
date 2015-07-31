require 'aruba/api'
require 'aruba/in_process'
require 'aruba/reporting'

# Dummy module so we can use NoSE::CLI
module NoSE
end
require 'nose/cli'

module NoSE::CLI
  class NoSECLI
    def initialize(*args)
      suppress_warnings do
        file = File.join File.dirname(__FILE__), '..', '..', 'nose.yml.example'
        NoSECLI.const_set 'CONFIG_FILE_NAME', file
      end

      super(*args)
    end

    # Override so we don't look like RSpec
    def self.basename
      'nose'
    end
  end

  # Runner for use with tests
  class Runner
    # Allow everything fun to be injected from the outside
    # while defaulting to normal implementations.
    def initialize(argv, stdin = STDIN, stdout = STDOUT, stderr = STDERR,
                   kernel = Kernel)
      @argv = argv
      @stdin = stdin
      @stdout = stdout
      @stderr = stderr
      @kernel = kernel
    end

    # Execute the app using the injected stuff
    # https://github.com/erikhuda/thor/wiki/Integrating-with-Aruba-In-Process-Runs
    def execute!
      exit_code = begin
                    # Thor accesses these streams directly rather than
                    # letting them be injected, so we replace them...
                    $stderr = @stderr
                    $stdin = @stdin
                    $stdout = @stdout

                    # Run our normal Thor app the way we know and love.
                    NoSE::CLI::NoSECLI.start @argv

                    # Thor::Base#start does not have a return value,
                    # assume success if no exception is raised.
                    0
                  rescue StandardError => e
                    # The Ruby interpreter would pipe this to STDERR
                    # and exit 1 in the case of an unhandled exception
                    b = e.backtrace
                    @stderr.puts "#{b.shift}: #{e.message} (#{e.class})"
                    @stderr.puts b.map { |s| "\tfrom #{s}" }.join("\n")
                    1
                  rescue SystemExit => e
                    e.status
                  ensure
                    $stderr = STDERR
                    $stdin = STDIN
                    $stdout = STDOUT
                  end

      # Proxy our exit code back to the injected kernel.
      @kernel.exit exit_code
    end
  end
end

Aruba::InProcess.main_class = NoSE::CLI::Runner
Aruba.process = Aruba::InProcess

RSpec.configure do |config|
  config.include Aruba::Api

  config.before(:each) do
    restore_env
    clean_current_dir
  end
end
