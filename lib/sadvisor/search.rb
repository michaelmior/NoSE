require 'erb'
require 'gurobi'
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
    # @return [Binding]
    def binding
      binding
    end
  end

  # Searches for the optimal indices for a given workload
  class Search
    def initialize(workload)
      @workload = workload
    end

    # Search for the best configuration of indices for a given space constraint
    # @return [Array<Index>]
    def search_all(max_space = Float::INFINITY, gap = 0.01)
      # Construct the simple indices for all entities and
      # remove this from the total size
      simple_indexes = @workload.entities.values.map(&:simple_index)
      simple_size = simple_indexes.map(&:size).inject(0, &:+)
      max_space -= simple_size
      return [] if max_space <= 0

      # Get the cost of all queries with the simple indices
      simple_planner = Planner.new @workload, simple_indexes
      simple_costs = {}
      @workload.queries.each do |query|
        simple_costs[query] = simple_planner.min_plan(query).cost
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
      solve_mpl 'schema_all', indexes, gap,
                max_space: max_space,
                benefits: benefits,
                configurations: configurations,
                index_sizes: index_sizes,
                configuration_sizes: configuration_sizes
    end

    # Search for optimal indices using an ILP which searches for
    # non-overlapping indices
    # @return [Array<Index>]
    def search_overlap(max_space = Float::INFINITY, gap = 0.01)
      # Construct the simple indices for all entities and
      # remove this from the total size
      simple_indexes = @workload.entities.values.map(&:simple_index)
      simple_size = simple_indexes.map(&:size).inject(0, &:+)
      max_space -= simple_size
      return [] if max_space <= 0

      # Generate all possible combinations of indices
      indexes = IndexEnumerator.new(@workload).indexes_for_workload.to_a
      index_sizes = indexes.map(&:size)

      # Get the cost of all queries with the simple indices
      simple_planner = Planner.new @workload, simple_indexes
      simple_costs = {}
      @workload.queries.each do |query|
        simple_costs[query] = simple_planner.min_plan(query).cost
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
          range1 = Search.send :index_range, entities, index1

          query_indices[j + 1..-1].each do |index2|
            range2 = Search.send :index_range, entities, index2
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

      # TODO: Add MathProg back as an option
      # # Generate the MathProg file and solve the program
      # solve_mpl 'schema_overlap', indexes, gap,
      #           max_space: max_space,
      #           index_sizes: index_sizes,
      #           query_overlap: query_overlap,
      #           benefits: benefits

      # Solve the LP using Gurobi
      solve_gurobi indexes,
                   max_space: max_space,
                   index_sizes: index_sizes,
                   query_overlap: query_overlap,
                   benefits: benefits
    end

    # Solve the index selection problem using Gurobi
    def solve_gurobi(indexes, data)
      model = Gurobi::Model.new(Gurobi::Env.new)
      model.getEnv.set_int(Gurobi::IntParam::OUTPUT_FLAG, 0)

      # Initialize query and index variables
      index_vars = []
      query_vars = []
      (0...indexes.length).each do |i|
        index_vars[i] = model.addVar(0, 1, 0, Gurobi::BINARY, "i#{i}")
        query_vars[i] = []
        (0...@workload.queries.length).each do |q|
          query_vars[i][q] = model.addVar(0, 1, 0, Gurobi::BINARY, "q#{q}i#{i}")

        end
      end

      # Add constraint for indices being present
      model.update
      (0...indexes.length).each do |i|
        (0...@workload.queries.length).each do |q|
          model.addConstr(query_vars[i][q] + -1 * index_vars[i] <= 0)
        end
      end

      # Add space constraint if needed
      if data[:max_space].finite?
        space = indexes.each_with_index.map do |index, i|
          (index.size * 1.0) * index_vars[i]
        end.reduce(&:+)
        model.addConstr(space <= data[:max_space] * 1.0)
      end

      # Add overlapping index constraints
      data[:query_overlap].each do |q, overlaps|
        overlaps.each do |i, overlap|
          overlap.each do |j|
            model.addConstr(query_vars[i][q] + query_vars[j][q] <= 1)
          end
        end
      end

      # Set the objective function
      max_benefit = (0...indexes.length).to_a \
                    .product((0...@workload.queries.length).to_a).map do |i, q|
        query_vars[i][q] * (data[:benefits][q][i] * 1.0)
      end.reduce(&:+)
      model.setObjective(max_benefit, Gurobi::MAXIMIZE)

      # Run the optimizer
      model.update
      model.optimize

      # Return the selected indices
      indexes.select.with_index do |_, i|
        index_vars[i].get_double(Gurobi::DoubleAttr::X) == 1.0
      end
    end

    # Create a new range over the entities traversed by an index using
    # the numerical indices into the query entity path
    def self.index_range(entities, index)
      Range.new(*(index.path.map do |entity|
        entities.index entity.name
      end).minmax)
    end
    private_class_method :index_range

    private

    # Get the reduction in cost from using each configuration of indices
    def benefits(combos, simple_costs)
      @workload.queries.map do |query|
        combos.map do |combo|
          combo_planner = Planner.new @workload, combo
          begin
            [0, simple_costs[query] - combo_planner.min_plan(query).cost].max
          rescue NoPlanException
            0
          end
        end
      end
    end

    # Solve the given MathProg program and return the output of the post-solver
    def solve_mpl(template_name, indexes, gap, data)
      namespace = Namespace.new(data)
      template_file = File.dirname(__FILE__) + "/#{template_name}.mod.erb"
      template = File.read(template_file)
      mpl = ERB.new(template, 0, '>').result(namespace.binding)

      # Solve the problem, which prints the solution
      file = Tempfile.new 'schema.mod'
      begin
        file.write mpl
        file.close

        Rglpk.disable_output
        tran = Rglpk::Workspace.new
        prob = tran.read_model file.path
        prob.simplex msg_lev: Rglpk::GLP_MSG_OFF
        prob.mip presolve: Rglpk::GLP_ON, msg_lev: Rglpk::GLP_MSG_OFF,
                 mip_gap: gap

        output = tran.postsolve prob
      ensure
        file.close
        file.unlink
      end

      output.split.map(&:to_i).map { |i| indexes[i - 1] }
    end
  end
end
