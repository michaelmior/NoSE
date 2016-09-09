require 'aruba/api'
require 'aruba/in_process'
require 'aruba/reporting'

# Dummy module so we can use NoSE::CLI
module NoSE
end
require 'nose/cli'

module NoSE
  module CLI
    class NoSECLI
      TEST_CONFIG_FILE_NAME = File.join File.dirname(__FILE__), '..', '..',
                                        'nose.yml.example'

      def initialize(*args)
        suppress_warnings do
          NoSECLI.const_set 'CONFIG_FILE_NAME', TEST_CONFIG_FILE_NAME
        end

        super(*args)
      end

      # Override so we don't look like RSpec
      def self.basename
        'nose'
      end

      # Override to allow mocking of produced classes
      no_commands do
        def get_backend(*_args)
          @backend
        end

        def get_class(name, _options)
          instance_variable_get "@#{name}_class".to_sym
        end
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
      # rubocop:disable Metrics/MethodLength
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
      # rubocop:enable Metrics/MethodLength
    end
  end
end

Aruba.configure do |config|
  config.main_class = NoSE::CLI::Runner
  config.command_launcher = :in_process
end

# Monkey patch Aruba so it uses the correct
# default value for dir_string in expand_path

module ArubaPatch
  def expand_path(file_name, dir_string = FakeFS::FileSystem.current_dir.to_s)
    super(file_name, dir_string)
  end
end

module Aruba
  module Api
    prepend ArubaPatch
  end
end

RSpec.configure do |config|
  config.include Aruba::Api

  config.before(:each) do
    restore_env
    setup_aruba
  end
end
