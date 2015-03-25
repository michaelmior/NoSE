require 'fileutils'

module NoSE::CLI
  # Add a command to reformat a plan file
  class NoSECLI < Thor
    desc 'search-all NAME DIRECTORY',
         'output all possible schemas for the workload NAME under different ' \
         'storage constraints to DIRECTORY'
    option :format, type: :string, default: 'txt',
                    enum: ['txt', 'json', 'yml'], aliases: '-f'
    def search_all(name, directory)
      # Load the workload and cost model and create the output directory
      workload = get_workload name
      config = load_config
      cost_model = get_class 'cost', config[:cost_model][:name]
      FileUtils::mkdir_p(directory) unless Dir.exists?(directory)

      # Start with the maximum possible size and divide in two
      max_result = search_result workload, cost_model
      max_size = max_result.total_size
      results = [max_result]
      sizes = Set.new [0, max_size]
      size_queue = [max_size / 2.0]
      until size_queue.empty?
        # Find a new size to examine
        size = size_queue.pop

        # Continue dividing the range of examined sizes
        next_size = sizes.sort.select { |n| n > size }.first
        next_size = (next_size - size) / 2.0 + size

        prev_size = sizes.sort.reverse.select { |n| n < size }.first
        prev_size = (size - prev_size) / 2.0 + prev_size

        begin
          result = search_result workload, cost_model, size
          next if sizes.include? result.total_size
        rescue NoSE::Search::NoSolutionException, NoSE::Plans::NoPlanException
          # No result was found, so only explore the larger side
        else
          # Add the smaller size to the queue and save the result
          size_queue.push prev_size unless sizes.include? prev_size
          results.push result
        end

        # Add the larger size to the queue
        size_queue.push(next_size) unless sizes.include? next_size

        # Note that we visited this size (and the result size)
        sizes.add size
        sizes.add result.total_size unless result.nil?
      end

      # Output all results to file
      results.sort_by!(&:total_size)
      results.each_with_index do |result, i|
        file = File.open File.join(directory, "#{i}.#{options[:format]}"), 'w'
        begin
          send(('output_' + options[:format]).to_sym, result, file)
        ensure
          file.close
        end
      end
    end
  end
end
