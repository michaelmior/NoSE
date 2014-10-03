require 'formatador'
require 'json'
require 'ostruct'
require 'parallel'
require 'thor'
require 'yaml'

module Sadvisor
  class SadvisorCLI < Thor
    class_option :debug, type: :boolean
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

    desc 'workload NAME', 'run the workload NAME'
    option :max_space, type: :numeric, default: Float::INFINITY
    option :format, type: :string, default: 'text'
    def workload(name)
      # rubocop:disable GlobalVars

      is_text = options[:format] == 'text'

      require_relative "../../workloads/#{name}"

      enumerated_indexes = Sadvisor::IndexEnumerator.new($workload) \
        .indexes_for_workload.to_a

      if options[:max_space].finite?
        indexes = Sadvisor::Search.new($workload) \
          .search_overlap(enumerated_indexes, options[:max_space])
      else
        indexes = enumerated_indexes.clone
      end

      planner = Sadvisor::Planner.new $workload, indexes
      plans = {}
      $workload.queries.each do |query|
        plans[query] = planner.min_plan query
      end

      indexes = plans.values.map(&:to_a).flatten.select do |step|
        step.is_a? Sadvisor::IndexLookupPlanStep
      end.map(&:index).to_set

      header = "Indexes\n" + '━' * 50
      Formatador.display_line "[blue]#{header}[/]" if is_text
      indexes.each do |index|
        puts index.inspect
      end if is_text
      puts if is_text

      total_size = indexes.map(&:size).inject(0, :+)
      Formatador.display_line "Total size: [blue]#{total_size}[/]\n" if is_text

      # Output queries plans for the discovered indices
      header = "Query plans\n" + '━' * 50
      Formatador.display_line "[blue]#{header}[/]" if is_text
      plans.each do |query, plan|
        puts query.inspect if is_text
        puts plan.inspect if is_text
        puts if is_text
      end

      if options[:format] == 'json'
        result = OpenStruct.new(
          workload: $workload,
          enumerated_indexes: enumerated_indexes,
          indexes: indexes.to_set,
          plans: plans.values,
          total_size: total_size,
          total_cost: $workload.query_weights.map do |query, weight|
            weight * plans[query].cost
          end.inject(0, &:+)
        )

        puts JSON.pretty_generate \
          Sadvisor::SearchResultRepresenter.represent(result).to_hash
      end

      # rubocop:enable GlobalVars
    end

    desc 'repl PLAN_FILE', 'start the REPL with the given PLAN_FILE'
    def repl(plan_file)
      result = load_results plan_file
      config = load_config
      backend = get_backend(config, result)

      loop do
        line = get_line
        break if line.nil?
        line.chomp!
        query = Statement.new line, result.workload

        # Execute the query
        Formatador.display_compact_table backend.query(query)
      end
    end

    desc 'create PLAN_FILE', 'create indexes from the given PLAN_FILE'
    option :dry_run, type: :boolean, default: false
    def create(plan_file)
      result = load_results(plan_file)
      config = load_config
      backend = get_backend(config, result)

      # Produce the DDL and execute unless the dry run option was given
      backend.indexes_ddl(!options[:dry_run]).each do |ddl|
        puts ddl
      end
    end

    desc 'load PLAN_FILE DIRECTORY', 'create indexes from the given PLAN_FILE'
    option :progress, type: :boolean, default: true
    def load(plan_file, directory)
      result = load_results(plan_file)
      config = load_config
      backend = get_backend(config, result)

      loader = CSVLoader.new result.workload, backend
      loader.load result.indexes, directory, options[:progress]
    end

    private

    # Load the configuration to use for a backend
    def load_config
      config = YAML.load_file File.join(Dir.pwd, 'sadvisor.yml')
      Hash[config.map { |k, v| [k.to_sym, v] }]
    end

    # Get a backend instance for a given configuration and dataset
    def get_backend(config, result)
      require_relative "backends/#{config[:database]}"
      be_class_name = ['Sadvisor', config[:database].capitalize + 'Backend']
      be_class_name.reduce(Object) do |mod, name_part|
        mod.const_get name_part
      end.new(result.workload, result.indexes, result.plans, **config)
    end

    # Load results of a previous search operation
    def load_results(plan_file)
      representer = Sadvisor::SearchResultRepresenter.represent(OpenStruct.new)
      json = File.read(plan_file)
      representer.from_json(json)
    end

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
