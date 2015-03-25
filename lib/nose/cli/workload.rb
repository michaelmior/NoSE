require 'formatador'
require 'ostruct'
require 'json'

module NoSE::CLI
  # Add a command to run the advisor for a given workload
  class NoSECLI < Thor
    desc 'workload NAME', 'run the workload NAME'
    option :max_space, type: :numeric, default: Float::INFINITY, aliases: '-s'
    option :format, type: :string, default: 'text',
                    enum: ['text', 'json', 'yaml'], aliases: '-f'
    option :output, type: :string, default: nil, aliases: '-o'
    def workload(name)
      # rubocop:disable GlobalVars
      require_relative "../../../workloads/#{name}"
      workload = $workload
      # rubocop:enable GlobalVars

      # Get the cost model
      config = load_config
      cost_model = get_class 'cost', config[:cost_model][:name]

      # Execute the advisor
      result = search workload, cost_model, options[:max_space]

      # Output the results in the specified format
      file = options[:output].nil? ? $stdout : File.open(options[:output], 'w')
      begin
        send(('output_' + options[:format]).to_sym, result, file)
      ensure
        file.close unless options[:output].nil?
      end
    end

    private

    # Find the indexes with the given space constraint
    def find_indexes(workload, enumerated_indexes, space, cost_model)
      if space.finite?
        NoSE::Search::Search.new(workload, cost_model) \
          .search_overlap enumerated_indexes, space
      else
        enumerated_indexes.clone
      end
    end
  end
end
