require 'erb'
require 'ostruct'
require 'tempfile'
require 'rglpk'

class Namespace
  def initialize(hash)
    hash.each do |key, value|
      singleton_class.send(:define_method, key) { value }
    end
  end

  def get_binding
    binding
  end
end

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

  # Search for the best configuration of indices for a given space constraint
  def search(max_space)
    # Construct the simple indices for all entities and
    # remove this from the total size
    simple_indexes = @workload.entities.values.map(&:simple_index)
    simple_size = simple_indexes.map(&:size).inject(0, &:+)
    max_space -= simple_size  # XXX need to check if max_space < simple_size

    # Get the cost of all queries with the simple indices
    simple_planner = Planner.new @workload, simple_indexes
    simple_costs = {}
    @workload.queries.map do |query|
      simple_costs[query] = simple_planner.min_query_cost query
    end

    # Generate all possible combinations of indices
    indexes = IndexEnumerator.indexes_for_workload @workload
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

    # Generate the MathProg file from the template
    namespace = Namespace.new(
      max_space: max_space,
      benefits: benefits,
      configurations: configurations,
      index_sizes: index_sizes,
      configuration_sizes: configuration_sizes
    )
    template_file = File.dirname(__FILE__) + '/schema.mod.erb'
    template = File.read(template_file)
    mpl = ERB.new(template, 0, '>').result(namespace.get_binding)
    file = Tempfile.new 'schema.mod'

    # Solve the problem, which prints the solution
    begin
      file.write mpl
      file.close

      Rglpk.disable_output
      tran = Rglpk::Workspace.new
      prob = tran.read_model file.path
      prob.simplex msg_lev: Rglpk::GLP_MSG_OFF
      prob.mip presolve: Rglpk::GLP_ON, msg_lev: Rglpk::GLP_MSG_OFF

      index_choices = tran.postsolve prob
    ensure
      file.close
      file.unlink
    end

    index_choices.split.map(&:to_i).map { |i| indexes[i - 1] }
  end
end
