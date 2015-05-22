# Statement planning and abstract models of execution steps
module NoSE::Plans
  # Ongoing state of a statement throughout the execution plan
  class StatementState
    attr_accessor :from, :fields, :eq, :range, :order_by, :path, :cardinality,
                  :given_fields
    attr_reader :statement, :model

    def initialize(statement, model)
      @statement = statement
      @model = model
      @from = statement.from

      populate_fields statement
      populate_conditions statement

      # Get the ordering from the query
      @order_by = statement.is_a?(NoSE::Query) ? statement.order.dup : []

      @cardinality = 1  # this will be updated on the first index lookup
      @given_fields = @eq.dup
    end

    # All the fields referenced anywhere in the statement
    def all_fields
      all_fields = @fields + @eq
      all_fields << @range unless @range.nil?
      all_fields
    end

    # :nocov:
    def to_color
      @statement.text +
        "\n  fields: " + @fields.map(&:to_color).to_a.to_color +
        "\n      eq: " + @eq.map(&:to_color).to_a.to_color +
        "\n   range: " + (@range.nil? ? '(nil)' : @range.name) +
        "\n   order: " + @order_by.map(&:to_color).to_a.to_color +
        "\n    path: " + @path.to_a.to_color
    end
    # :nocov:

    # Check if the statement has been fully answered
    # @return [Boolean]
    def answered?(check_limit: true)
      done = @fields.empty? && @eq.empty? && @range.nil? && @order_by.empty?
      return done unless statement.is_a? NoSE::Query

      # Check if the limit has been applied
      done &&= @cardinality <= @statement.limit unless @statement.limit.nil? ||
                                                       !check_limit

      done
    end

    # Get all fields relevant for filtering in the statement for entities
    # in the given list, optionally including selected fields
    # @return [Array<Field>]
    def fields_for_entities(entities, select: false)
      path_fields = @eq + @order_by

      # If necessary, include ALL the fields which should be selected,
      # otherwise we can exclude fields from the last entity set since
      # we may end up selecting these with a separate index lookup
      if select
        path_fields += @fields
      else
        path_fields += @fields.select do
          |field| entities[0..-2].include? field.parent
        end
      end

      path_fields << @range unless @range.nil?
      path_fields.select { |field| entities.include? field.parent }
    end

    private

    # Populate the fields used in this statement
    def populate_fields(statement)
      # TODO Update for relationship changes
      case statement
      when NoSE::Query then @fields = statement.select
      when NoSE::Delete then @fields = []
      else @fields = statement.settings.map(&:field)
      end
    end

    # Populate the conditions and path of the statement
    def populate_conditions(statement)
      # TODO Update for relationship changes
      if statement.is_a? NoSE::Insert
        @eq = []
        @range = nil
        @order_by = []
        @path = [statement.entity]
      else
        @eq = statement.eq_fields.dup
        @range = statement.range_field
        @path = statement.longest_entity_path.reverse
      end
    end
  end

  # A tree of possible query plans
  class QueryPlanTree
    include Enumerable

    attr_reader :root
    attr_accessor :cost_model

    def initialize(state, cost_model)
      @root = RootPlanStep.new(state)
      @cost_model = cost_model
    end

    # Enumerate all plans in the tree
    def each
      nodes = [@root]

      while nodes.length > 0
        node = nodes.pop
        if node.children.length > 0
          nodes.concat node.children.to_a
        else
          # This is just an extra check to make absolutely
          # sure we never consider invalid statement plans
          fail unless node.state.answered?

          yield node.parent_steps @cost_model
        end
      end
    end

    # Return the total number of plans for this statement
    # @return [Integer]
    def size
      to_a.count
    end

    # :nocov:
    def to_color(step = nil, indent = 0)
      step = @root if step.nil?
      '  ' * indent + step.to_color + "\n" + step.children.map do |child_step|
        to_color child_step, indent + 1
      end.reduce('', &:+)
    end
    # :nocov:
  end

  # Thrown when it is not possible to construct a plan for a statement
  class NoPlanException < StandardError
  end

  # A single plan for a statement
  class StatementPlan
    attr_accessor :statement
    attr_accessor :cost_model

    include Comparable
    include Enumerable

    # Most of the work is delegated to the array
    extend Forwardable
    def_delegators :@steps, :each, :<<, :[], :==, :===, :eql?,
      :inspect, :to_s, :to_a, :to_ary, :last, :length, :count

    def initialize(statement, cost_model)
      @steps = []
      @statement = statement
      @cost_model = cost_model
    end

    # Two plans are compared by their execution cost
    def <=>(other)
      cost <=> other.cost
    end

    # The estimated cost of executing the statement using this plan
    # @return [Numeric]
    def cost
      @steps.map { |step| step.cost @cost_model }.inject(0, &:+)
    end
  end

  # A single step in a statement plan
  class PlanStep
    include Supertype

    attr_accessor :state, :parent
    attr_reader :children, :fields

    def initialize
      @children = Set.new
      @parent = nil
      @fields = Set.new
    end

    # :nocov:
    def to_color
      # Split on capital letters and remove the last two parts (PlanStep)
      self.class.name.split('::').last.split(/(?=[A-Z])/)[0..-3] \
        .map(&:downcase).join(' ').capitalize
    end
    # :nocov:

    def children=(children)
      @children = children.to_set

      # Track the parent step of each step
      children.each do |child|
        child.instance_variable_set(:@parent, self)
        fields = child.instance_variable_get(:@fields) + self.fields
        child.instance_variable_set(:@fields, fields)
      end
    end

    # Mark the fields in this index as fetched
    def add_fields_from_index(index)
      @fields += index.all_fields
    end

    # Get the list of steps which led us here
    # If a cost model is not provided, statement plans using
    # this step cannot be evaluated on the basis of cost
    #
    # (this is to support PlanStep#parent_index which does not need cost)
    # @return [StatementPlan]
    def parent_steps(cost_model = nil)
      steps = nil

      if @parent.nil?
        steps = StatementPlan.new state.statement, cost_model
      else
        steps = @parent.parent_steps cost_model
        steps << self
      end

      steps
    end

    # Find the closest index to this step
    def parent_index
      step = parent_steps.to_a.reverse.find do |parent_step|
        parent_step.is_a? IndexLookupPlanStep
      end
      step.index unless step.nil?
    end

    # The cost of executing this step in the plan
    # @return [Numeric]
    def cost(cost_model)
      cost_model.method((subtype_name + '_cost').to_sym).call self
    end

    # Add the Subtype module to all step classes
    def self.inherited(child_class)
      child_class.send(:include, Subtype)
    end
  end

  # A dummy step used to inspect failed statement plans
  class PrunedPlanStep < PlanStep
    def state
      OpenStruct.new answered?: true
    end
  end

  # The root of a tree of statement plans used as a placeholder
  class RootPlanStep < PlanStep
    def initialize(state)
      super()
      @state = state
    end
  end
end

require_relative 'plans/filter'
require_relative 'plans/index_lookup'
require_relative 'plans/limit'
require_relative 'plans/sort'
require_relative 'plans/update'

require_relative 'plans/query_planner'
require_relative 'plans/update_planner'
