require 'formatador'
require 'parallel'
require 'thor'
require 'yaml'

module NoSE::CLI
  # A command-line interface to running the advisor tool
  class NoSECLI < Thor
    class_option :debug, type: :boolean, aliases: :d
    class_option :parallel, type: :boolean, default: true
    class_option :colour, type: :boolean, default: nil, aliases: :C

    def initialize(*args)
      super

      # Enable forcing the colour or no colour for output
      # We just lie to Formatador about whether or not $stdout is a tty
      unless options[:colour].nil?
        stdout_metaclass = class << $stdout; self; end
        method = options[:colour] ? ->() { true } : ->() { false }
        stdout_metaclass.send(:define_method, :tty?, &method)
      end

      # Disable parallel processing if desired
      Parallel.instance_variable_set(:@processor_count, 0) \
        unless options[:parallel]
    end

    private

    # Load the configuration to use for a backend
    def load_config
      config = YAML.load_file File.join(Dir.pwd, 'nose.yml')
      config.deep_symbolize_keys
    end

    # Get a backend instance for a given configuration and dataset
    def get_backend(config, result)
      require_relative "backends/#{config[:database]}"
      be_class_name = ['NoSE', 'Backends',
                       config[:database].capitalize + 'Backend']
      be_class_name.reduce(Object) do |mod, name_part|
        mod.const_get name_part
      end.new(result.workload, result.indexes, result.plans, config[:backend])
    end

    # Find the appropriate loader class based on the config
    def get_loader_class(config)
      get_class 'loaders', NoSE::Loader, :load, config[:loader][:name]
    end

    # Find the appropriate proxy class based on the config
    def get_proxy_class(config)
      get_class 'proxy', NoSE::Proxy, :handle_connection,
                config[:proxy][:name]
    end

    # Find the appropriate loader class based on the config
    def get_class(path, parent_class, method, name)
      require_relative "#{path}/#{name}"
      loaders = ObjectSpace.each_object(parent_class.singleton_class)
      loaders.find do |klass|
        path = klass.instance_method(method).source_location
        next if path.nil?
        !/\b#{name}\.rb/.match(path.first).nil?
      end
    end

    # Load results of a previous search operation
    def load_results(plan_file)
      representer = NoSE::SearchResultRepresenter.represent(OpenStruct.new)
      json = File.read(plan_file)
      representer.from_json(json)
    end
  end
end

# Require the various subcommands
require_relative 'cli/create'
require_relative 'cli/load'
require_relative 'cli/genworkload'
require_relative 'cli/graph'
require_relative 'cli/proxy'
require_relative 'cli/repl'
require_relative 'cli/workload'
