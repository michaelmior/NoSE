# frozen_string_literal: true

require 'fileutils'

module NoSE
  module CLI
    # Add a command to reformat a plan file
    class NoSECLI < Thor
      desc 'search-all NAME DIRECTORY',
           'output all possible schemas for the workload NAME under ' \
           'different storage constraints to DIRECTORY'

      long_desc <<-LONGDESC
        `nose search-all` is a convenience for executing `nose search` with a
        variety of different storage constraints. It will start with the
        smallest and largest possible schemas and then perform a binary search
        throughout the search space to discover schemas of various sizes.
      LONGDESC

      shared_option :format
      shared_option :mix

      option :enumerated, type: :boolean, aliases: '-e',
                          desc: 'whether enumerated indexes should be output'
      option :read_only, type: :boolean, default: false,
                         desc: 'whether to ignore update statements'
      option :max_results, type: :numeric, default: Float::INFINITY,
                           aliases: '-n',
                           desc: 'the maximum number of results to produce'
      def search_all(name, directory)
        # Load the workload and cost model and create the output directory
        workload = Workload.load name
        workload.mix = options[:mix].to_sym \
          unless options[:mix] == 'default' && workload.mix != :default
        workload.remove_updates if options[:read_only]
        cost_model = get_class_from_config options, 'cost', :cost_model
        FileUtils.mkdir_p(directory) unless Dir.exist?(directory)

        # Run the search and output the results
        results = search_results workload, cost_model, options[:max_results]
        output_results results, directory, options
      end

      private

      # Get a list of all possible search results
      # @return [Array<Search::Results>]
      def search_results(workload, cost_model, max_results)
        # Start with the maximum possible size and divide in two
        max_result = search_result workload, cost_model
        max_size = max_result.total_size
        min_result = search_result workload, cost_model,
                                   Float::INFINITY, Search::Objective::SPACE
        min_size = min_result.total_size
        min_result = search_result workload, cost_model, min_size

        # If we only have one result, return
        return [max_result] if max_size == min_size

        results = [max_result, min_result]
        num_results = 2
        sizes = Set.new [min_size, max_size]
        size_queue = [(max_size - min_size) / 2.0 + min_size]
        until size_queue.empty?
          # Stop if we found the appropriate number of results
          return results if num_results >= max_results

          # Find a new size to examine
          size = size_queue.pop

          # Continue dividing the range of examined sizes
          next_size = sizes.sort.detect { |n| n > size }
          next_size = (next_size - size) / 2.0 + size

          prev_size = sizes.sort.reverse_each.detect { |n| n < size }
          prev_size = (size - prev_size) / 2.0 + prev_size

          begin
            @logger.info "Running search with size #{size}"

            result = search_result workload, cost_model, size
            next if sizes.include?(result.total_size) || result.nil?
          rescue Search::NoSolutionException, Plans::NoPlanException
            # No result was found, so only explore the larger side
            @logger.info "No solution for size #{size}"
          else
            # Add the smaller size to the queue and save the result
            size_queue.push prev_size unless sizes.include? prev_size
            results.push result
            num_results += 1
          end

          # Add the larger size to the queue
          size_queue.push(next_size) unless sizes.include? next_size

          # Note that we visited this size (and the result size)
          sizes.add size
          sizes.add result.total_size unless result.nil?
        end

        results
      end

      # Output all results to file
      # @return [void]
      def output_results(results, directory, options)
        results.sort_by!(&:total_size)
        results.each_with_index do |result, i|
          file = File.open File.join(directory, "#{i}.#{options[:format]}"), 'w'
          begin
            send(('output_' + options[:format]).to_sym,
                 result, file, options[:enumerated])
          ensure
            file.close
          end
        end
      end
    end
  end
end
