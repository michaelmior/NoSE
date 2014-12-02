require 'formatador'
require 'parallel'
require 'thor'
require 'yaml'

module Sadvisor
  # A command-line interface to running the advisor tool
  class SadvisorCLI < Thor
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
      config = YAML.load_file File.join(Dir.pwd, 'sadvisor.yml')
      config.deep_symbolize_keys
    end

    # Get a backend instance for a given configuration and dataset
    def get_backend(config, result)
      require_relative "backends/#{config[:database]}"
      be_class_name = ['Sadvisor', config[:database].capitalize + 'Backend']
      be_class_name.reduce(Object) do |mod, name_part|
        mod.const_get name_part
      end.new(result.workload, result.indexes, result.plans, config[:backend])
    end

    # Load results of a previous search operation
    def load_results(plan_file)
      representer = Sadvisor::SearchResultRepresenter.represent(OpenStruct.new)
      json = File.read(plan_file)
      representer.from_json(json)
    end
  end
end

# Require the various subcommands
require_relative 'cli/create'
require_relative 'cli/load'
require_relative 'cli/graph'
require_relative 'cli/repl'
require_relative 'cli/workload'
