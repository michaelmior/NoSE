require 'formatador'
require 'parallel'
require 'thor'
require 'yaml'

module NoSE::CLI
  # A command-line interface to running the advisor tool
  class NoSECLI < Thor
    check_unknown_options!

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
      be_class = get_class 'backend', config
      be_class.new result.workload, result.indexes, result.plans,
                   config[:backend]
    end

    # Get a
    def get_class(class_name, config)
      name = config
      name = config[class_name.to_sym][:name] if config.is_a? Hash
      require_relative "#{class_name}/#{name}"
      full_class_name = ['NoSE', class_name.capitalize,
                         name.capitalize + class_name.capitalize]
      full_class_name.reduce(Object) do |mod, name_part|
        mod.const_get name_part
      end
    end

    # Load results of a previous search operation
    def load_results(plan_file)
      representer = NoSE::Serialize::SearchResultRepresenter.represent \
        OpenStruct.new
      json = File.read(plan_file)
      representer.from_json(json)
    end

    # Output the results of advising as text
    def output_text(result)
      # Output selected indexes
      header = "Indexes\n" + '━' * 50
      Formatador.display_line "[blue]#{header}[/]"
      result.indexes.each do |index|
        puts index.inspect
      end

      Formatador.display_line "Total size: [blue]#{result.total_size}[/]\n"
      puts

      # Output queries plans for the discovered indices
      header = "Query plans\n" + '━' * 50
      Formatador.display_line "[blue]#{header}[/]"
      result.plans.each do |plan|
        puts plan.query.inspect
        puts plan.inspect
        puts
      end
    end

    # Output the results of advising as JSON
    def output_json(result)
      puts JSON.pretty_generate \
        NoSE::Serialize::SearchResultRepresenter.represent(result).to_hash
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
require_relative 'cli/recost'
require_relative 'cli/workload'
