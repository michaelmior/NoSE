module Sadvisor
  # A simple representation of a random ER diagram
  class Network
    def initialize(params = {})
      @beta = params.fetch :beta, 0.5
      @nodes_nb = params.fetch :nodes_nb, 100
      @node_degree = params.fetch :node_degree, 3
      @field_count = RandomGaussian.new params.fetch(:num_fields, 5), 1

      @nodes = 0..(@nodes_nb - 1)
      num_entities = RandomGaussian.new 10_000, 100
      @entities = @nodes.map do |node|
        Sadvisor::Entity.new(random_name(node)) * num_entities.rand
      end

      @neighbours = Array.new(@nodes_nb) { Set.new }

      pick_fields
      build_initial_links
      rewire_links
      add_foreign_keys
    end

    def inspect
      @nodes.map do |node|
        @entities[node].inspect
      end.join "\n"
    end

    private

    FIELD_TYPES = [
      [IntegerField, 0.45],
      [StringField,  0.35],
      [DateField,    0.1],
      [FloatField,   0.1]
    ]

    # Select random fields for each entity
    def pick_fields
      @nodes.each do |node|
        @entities[node] << IDField.new('ID')
        0.upto(@field_count.rand).each do |field_index|
          type_rand = rand
          field = FIELD_TYPES.find do |_, threshold|
            type_rand -= threshold
            type_rand <= threshold
          end[0].send(:new, random_name(field_index))
          @entities[node] << field
        end
      end
    end

    # Add foreign key relationships for neighbouring nodes
    def add_foreign_keys
      @neighbours.each_with_index do |other_nodes, node|
        other_nodes.each do |other_node|
          if rand > 0.5
            from_node = node
            to_node = other_node
          else
            from_node = other_node
            to_node = node
          end

          @entities[from_node] << ForeignKey.new(
            @entities[to_node].name + 'ID',
            @entities[to_node])
        end
      end
    end

    # Add a new link between two nodes
    def add_link(node, other_node)
      @neighbours[node] << other_node
      @neighbours[other_node] << node
    end

    # Set up the initial links between all nodes
    def build_initial_links
      @nodes.each do |node|
        (@node_degree / 2).times do |i|
          add_link node, (node + i + 1) % @nodes_nb
        end
      end
    end

    # Remove a link between two nodes
    def remove_link(node, other_node)
      @neighbours[node].delete other_node
      @neighbours[other_node].delete node
    end

    # Rewire all links between nodes
    def rewire_links
      (@node_degree / 2).times do |i|
        @nodes.each do |node|
          if rand < @beta
            neighbour = (node + i + 1) % @nodes_nb
            remove_link node, neighbour

            unlinkable_nodes = [node, neighbour] + @neighbours[node].to_a
            new_neighbour = (@nodes.to_a - unlinkable_nodes).sample
            add_link node, new_neighbour
          end
        end
      end
    end

    VARIABLE_NAMES = %w(Foo Bar Baz Quux Corge Grault Garply Waldo Fred Plugh)

    # Generate a random name for an attribute
    def random_name(index)
      index.to_s.chars.map(&:to_i).map { |digit| VARIABLE_NAMES[digit] }.join
    end
  end
end

# Generate random numbers according to a Guassian distribution
class RandomGaussian
  def initialize(mean, stddev, integer = true, min = 1)
    @mean = mean
    @stddev = stddev
    @valid = false
    @next = 0
    @integer = integer
    @min = min
  end

  # Return the next valid random number
  def rand
    if @valid
      @valid = false
      clamp @next
    else
      @valid = true
      x, y = self.class.gaussian(@mean, @stddev)
      @next = y
      clamp x
    end
  end

  private

  # Clamp the value to the given minimum
  def clamp(value)
    value = value.to_i if @integer
    [@min, value].max unless @min.nil?
  end

  # Return a random number for the given distribution
  def self.gaussian(mean, stddev)
    theta = 2 * Math::PI * rand
    rho = Math.sqrt(-2 * Math.log(1 - rand))
    scale = stddev * rho
    x = mean + scale * Math.cos(theta)
    y = mean + scale * Math.sin(theta)
    [x, y]
  end
end
