require 'erb'
require 'ostruct'
require 'tempfile'
require 'rglpk'

module Sadvisor
  # Simple wrapper for ERB template isolation
  class Namespace
    def initialize(hash)
      hash.each do |key, value|
        singleton_class.send(:define_method, key) { value }
      end
    end

    # Return a binding within the class instance
    def get_binding
      binding
    end
  end

  # Searches for the optimal indices for a given workload
  class Search
    def initialize(workload)
      @workload = workload
    end

    # Get the reduction in cost from using each configuration of indices
    def benefits(combos, simple_costs)
      @workload.queries.map do |query|
        combos.map do |combo|
          combo_planner = Planner.new @workload, combo
          begin
            [0, simple_costs[query] - combo_planner.min_query_cost(query)].max
          rescue NoPlanException
            0
          end
        end
      end
    end

    # Solve the given MathProg program and return the output of the post-solver
    def solve_mpl(template_name, indexes, data)
      namespace = Namespace.new(data)
      template_file = File.dirname(__FILE__) + "/#{template_name}.mod.erb"
      template = File.read(template_file)
      mpl = ERB.new(template, 0, '>').result(namespace.get_binding)

      # Solve the problem, which prints the solution
      file = Tempfile.new 'schema.mod'
      begin
        file.write mpl
        file.close

        Rglpk.disable_output
        tran = Rglpk::Workspace.new
        prob = tran.read_model file.path
        prob.simplex msg_lev: Rglpk::GLP_MSG_OFF
        prob.mip presolve: Rglpk::GLP_ON, msg_lev: Rglpk::GLP_MSG_OFF

        output = tran.postsolve prob
      ensure
        file.close
        file.unlink
      end

      output.split.map(&:to_i).map { |i| indexes[i - 1] }
    end

    # Search for the best configuration of indices for a given space constraint
    def search_all(max_space)
      # Construct the simple indices for all entities and
      # remove this from the total size
      simple_indexes = @workload.entities.values.map(&:simple_index)
      simple_size = simple_indexes.map(&:size).inject(0, &:+)
      max_space -= simple_size  # XXX need to check if max_space < simple_size

      # Get the cost of all queries with the simple indices
      simple_planner = Planner.new @workload, simple_indexes
      simple_costs = {}
      @workload.queries.each do |query|
        simple_costs[query] = simple_planner.min_query_cost query
      end

      # Generate all possible combinations of indices
      indexes = IndexEnumerator.new(@workload).indexes_for_workload.to_a
      index_sizes = indexes.map(&:size)

      combos = 1.upto(indexes.count).map do |n|
        indexes.combination(n).to_a
      end.inject([], &:+)
      configuration_sizes = combos.map do |config|
        config.map(&:size).inject(0, :+)
      end

      benefits = benefits combos, simple_costs

      # Configurations are a list of list of numerical indices into the array
      # of query indices
      configurations = combos.map do |combo|
        combo.map { |index| indexes.index(index) + 1 }
      end

      # Generate the MathProg file and solve the program
      solve_mpl 'schema_all', indexes,
                max_space: max_space,
                benefits: benefits,
                configurations: configurations,
                index_sizes: index_sizes,
                configuration_sizes: configuration_sizes
    end

    # Create a new range over the entities traversed by an index using
    # the numerical indices into the query entity path
    def self.index_range(entities, index)
      Range.new(*(index.entities.map do |entity|
        entities.index entity.name
      end).minmax)
    end

    # Search for optimal indices using an ILP which searches for
    # non-overlapping indices
    def search_overlap(max_space)
      # Construct the simple indices for all entities and
      # remove this from the total size
      simple_indexes = @workload.entities.values.map(&:simple_index)
      simple_size = simple_indexes.map(&:size).inject(0, &:+)
      max_space -= simple_size  # XXX need to check if max_space < simple_size

      # Generate all possible combinations of indices
      indexes = IndexEnumerator.new(@workload).indexes_for_workload.to_a
      index_sizes = indexes.map(&:size)

      # Get the cost of all queries with the simple indices
      simple_planner = Planner.new @workload, simple_indexes
      simple_costs = {}
      @workload.queries.each do |query|
        simple_costs[query] = simple_planner.min_query_cost query
      end

      benefits = benefits indexes.map { |index| simple_indexes + [index] },
                          simple_costs

      query_overlap = {}
      @workload.queries.each_with_index do |query, i|
        entities = query.longest_entity_path
        query_indices = benefits[i].each_with_index.map do |benefit, j|
          benefit > 0 ? indexes[j] : nil
        end.compact
        query_indices.each_with_index do |index1, j|
          range1 = Search.index_range entities, index1

          query_indices[j + 1..-1].each do |index2|
            range2 = Search.index_range entities, index2
            unless (range1.to_a & range2.to_a).empty?
              overlap1 = indexes.index(index1)
              overlap2 = indexes.index(index2)
              query_overlap[i] = {} unless query_overlap.key?(i)
              if query_overlap[i].key? overlap1
                query_overlap[i][overlap1] << overlap2
              else
                query_overlap[i][overlap1] = [overlap2]
              end
            end
          end
        end
      end

      # Generate the MathProg file and solve the program
      solve_mpl 'schema_overlap', indexes,
                max_space: max_space,
                index_sizes: index_sizes,
                query_overlap: query_overlap,
                benefits: benefits
    end
  end
end
