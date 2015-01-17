module NoSE
  # A simple representation of a random ER diagram
  class Network
    attr_reader :entities

    def initialize(params = {})
      @beta = params.fetch :beta, 0.5
      @nodes_nb = params.fetch :nodes_nb, 10
      @node_degree = params.fetch :node_degree, 3
      @field_count = RandomGaussian.new params.fetch(:num_fields, 5), 1

      @nodes = 0..(@nodes_nb - 1)
      num_entities = RandomGaussian.new 10_000, 100
      @entities = @nodes.map do |node|
        NoSE::Entity.new('E' + random_name(node)) * num_entities.rand
      end

      @neighbours = Array.new(@nodes_nb) { Set.new }

      pick_fields
      build_initial_links
      rewire_links
      add_foreign_keys
    end

    # :nocov:
    def inspect
      @nodes.map do |node|
        @entities[node].inspect
      end.join "\n"
    end
    # :nocov:

    private

    # Probabilities of selecting various field types
    FIELD_TYPES = [
      [Fields::IntegerField, 0.45],
      [Fields::StringField,  0.35],
      [Fields::DateField,    0.1],
      [Fields::FloatField,   0.1]
    ]

    # Select random fields for each entity
    def pick_fields
      @nodes.each do |node|
        @entities[node] << Fields::IDField.new(@entities[node].name + 'ID')
        0.upto(@field_count.rand).each do |field_index|
          type_rand = rand
          field = FIELD_TYPES.find do |_, threshold|
            type_rand -= threshold
            type_rand <= threshold
          end[0].send(:new, 'F' + random_name(field_index))
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

          @entities[from_node] << Fields::ForeignKeyField.new(
            'FK' + @entities[to_node].name + 'ID',
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

    # Random names of variables combined to create random names
    VARIABLE_NAMES = %w(Foo Bar Baz Quux Corge Grault Garply Waldo Fred Plugh)

    # Generate a random name for an attribute
    def random_name(index)
      index.to_s.chars.map(&:to_i).map { |digit| VARIABLE_NAMES[digit] }.join
    end
  end

  # Generates random queries over entities in a given model
  class QueryGenerator
    def initialize(model)
      @model = model
    end

    # Generate a new random query from entities in the model
    # @return Query
    def random_query
      path = random_path(4)
      select = path.first.fields.values.sample 2
      conditions = 1.upto(3).map do
        path.sample.fields.values.sample
      end

      query = "SELECT #{select.map(&:name).join ', '} " \
              "FROM #{path.first.name} WHERE #{conditions.map do |field|
                "#{condition_field_name field, path}.#{field.name} = ?"
              end.join ' AND '}"

      Query.new query, @model
    end

    private

    # Get the name to be used in the query for a condition field
    # @return String
    def condition_field_name(field, path)
      field_path = path.first.name
      path_end = path.index(field.parent)
      last_entity = path.first
      path[1..path_end].each do |entity|
        field_path += '.' + last_entity.foreign_key_for(entity).name
        last_entity = entity
      end

      field_path
    end

    # Return a random path through the entity graph
    # @return [Array<Entity>]
    def random_path(max_length)
      path = [@model.entities.values.sample]
      while path.length < max_length
        keys = path.last.foreign_keys - path
        break if keys.empty?

        # Ensure we don't have cycles on the path
        next_entity = keys.sample.entity
        break if path.include? next_entity
        path << next_entity
      end

      path
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
