require 'pickup'

module NoSE
  # A simple representation of a random ER diagram
  class Network
    attr_reader :entities

    def initialize(params = {})
      @beta = params.fetch :beta, 0.5
      @nodes_nb = params.fetch :nodes_nb, 10
      @node_degree = params.fetch :node_degree, 3
      @field_count = RandomGaussian.new params.fetch(:num_fields, 5), 1
      @neighbours = Array.new(@nodes_nb) { Set.new }

      create_entities
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

    # Create random entities to use in the model
    # @return [void]
    def create_entities
      @nodes = 0..(@nodes_nb - 1)
      num_entities = RandomGaussian.new 10_000, 100
      @entities = @nodes.map do |node|
        Entity.new('E' + random_name(node)) * num_entities.rand
      end
    end

    # Probabilities of selecting various field types
    FIELD_TYPES = [
      [Fields::IntegerField, 0.45],
      [Fields::StringField,  0.35],
      [Fields::DateField,    0.1],
      [Fields::FloatField,   0.1]
    ]

    # Select random fields for each entity
    # @return [void]
    def pick_fields
      @nodes.each do |node|
        @entities[node] << Fields::IDField.new(@entities[node].name + 'ID')
        0.upto(@field_count.rand).each do |field_index|
          @entities[node] << random_field(field_index)
        end
      end
    end

    # Generate a random field to add to an entity
    # @return [Fields::Field]
    def random_field(field_index)
      type_rand = rand
      FIELD_TYPES.find do |_, threshold|
        type_rand -= threshold
        type_rand <= threshold
      end[0].send(:new, 'F' + random_name(field_index))
    end

    # Add foreign key relationships for neighbouring nodes
    # @return [void]
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

          from_field = Fields::ForeignKeyField.new(
            'FK' + @entities[to_node].name + 'ID',
            @entities[to_node])
          to_field = Fields::ForeignKeyField.new(
            'FK' + @entities[from_node].name + 'ID',
            @entities[from_node])

          from_field.reverse = to_field
          to_field.reverse = from_field

          @entities[from_node] << from_field
          @entities[to_node] << to_field
        end
      end
    end

    # Add a new link between two nodes
    # @return [void]
    def add_link(node, other_node)
      @neighbours[node] << other_node
      @neighbours[other_node] << node
    end

    # Set up the initial links between all nodes
    # @return [void]
    def build_initial_links
      @nodes.each do |node|
        (@node_degree / 2).times do |i|
          add_link node, (node + i + 1) % @nodes_nb
        end
      end
    end

    # Remove a link between two nodes
    # @return [void]
    def remove_link(node, other_node)
      @neighbours[node].delete other_node
      @neighbours[other_node].delete node
    end

    # Rewire all links between nodes
    # @return [void]
    def rewire_links
      (@node_degree / 2).times do |i|
        @nodes.each do |node|
          next unless rand < @beta

          neighbour = (node + i + 1) % @nodes_nb
          remove_link node, neighbour
          add_link node, new_neighbour(node, neighbour)
        end
      end
    end

    # Find a new neighbour for a node
    def new_neighbour(node, neighbour)
      unlinkable_nodes = [node, neighbour] + @neighbours[node].to_a
      (@nodes.to_a - unlinkable_nodes).sample
    end

    # Random names of variables combined to create random names
    VARIABLE_NAMES = %w(Foo Bar Baz Quux Corge Grault Garply Waldo Fred Plugh)

    # Generate a random name for an attribute
    # @return [String]
    def random_name(index)
      index.to_s.chars.map(&:to_i).map { |digit| VARIABLE_NAMES[digit] }.join
    end
  end

  # Generates random queries over entities in a given model
  class StatementGenerator
    def initialize(model)
      @model = model
    end

    # Generate a new random insertion to entities in the model
    # @return [Insert]
    def random_insert(connect = true)
      entity = @model.entities.values.sample
      settings = entity.fields.each_value.map do |field|
        "#{field.name}=?"
      end.join ', '
      insert = "INSERT INTO #{entity.name} SET #{settings} "

      # Optionally add connections to other entities
      insert += random_connection(entity) if connect

      Insert.new insert, @model
    end

    # Generate a random connection for an Insert
    def random_connection(entity)
      connections = entity.foreign_keys.values.sample(2)
      'AND CONNECT TO ' + connections.map do |connection|
        "#{connection.name}(?)"
      end.join(', ')
    end

    # Generate a new random update of entities in the model
    # @return [Update]
    def random_update(path_length = 1, updated_fields = 2, condition_count = 1)
      path = random_path(path_length)
      settings = random_settings path, updated_fields
      from = [path.first.parent.name] + path.entries[1..-1].map(&:name)
      update = "UPDATE #{from.first} FROM #{from.join '.'} SET #{settings} " +
               random_where_clause(path, condition_count)

      Update.new update, @model
    end

    # Get random settings for an update
    # @return [String]
    def random_settings(path, updated_fields)
      # Don't update key fields
      update_fields = path.entities.first.fields.values
      update_fields.reject! { |field| field.is_a? Fields::IDField }

      update_fields.sample(updated_fields).map do |field|
        "#{field.name}=?"
      end.join ', '
    end

    # Generate a new random deletion of entities in the model
    # @return [Delete]
    def random_delete
      path = random_path(1)

      from = [path.first.parent.name] + path.entries[1..-1].map(&:name)
      delete = "DELETE #{from.first} FROM #{from.join '.'} " +
               random_where_clause(path, 1)

      Delete.new delete, @model
    end

    # Generate a new random query from entities in the model
    # @return [Query]
    def random_query(path_length = 3, selected_fields = 2, condition_count = 2)
      path = random_path(path_length)
      select_fields = random_select(path, selected_fields)
      from = [path.first.parent.name] + path.entries[1..-1].map(&:name)
      query = "SELECT #{select_fields} FROM #{from.join '.'} " +
              random_where_clause(path, condition_count)

      Query.new query, @model
    end

    # Get random fields to select for a Query
    # @return [String]
    def random_select(path, selected_fields)
      path.entities.first.fields.values.sample(selected_fields).map do |field|
        path.entities.first.name + '.' + field.name
      end.join ', '
    end

    # Produce a random statement according to a given set of weights
    # @return [Statement]
    def random_statement(weights = { query: 80, insert: 10, update: 5,
                                     delete: 5 })
      pick = Pickup.new(weights)
      type = pick.pick(1)
      send(('random_' + type.to_s).to_sym)
    end

    # Return a random path through the entity graph
    # @return [KeyPath]
    def random_path(max_length)
      path = [@model.entities.values.sample.id_field]
      while path.length < max_length
        # Find a list of keys to entities we have not seen before
        last_entity = path.last.entity
        keys = last_entity.foreign_keys.values
        keys.reject! { |key| path.map(&:entity).include? key.entity }
        break if keys.empty?

        # Add a random new key to the path
        path << keys.sample
      end

      KeyPath.new path
    end

    # Produce a random query graph over the entity graph
    def random_graph(max_nodes)
      graph = QueryGraph::Graph.new
      last_node = graph.add_node @model.entities.values.sample
      while graph.size < max_nodes
        # Get the possible foreign keys to use
        keys = last_node.entity.foreign_keys.values
        keys.reject! { |key| graph.nodes.map(&:entity).include? key.entity }
        break if keys.empty?

        # Pick a random foreign key to traverse
        next_key = keys.sample
        graph.add_edge last_node, next_key.entity, next_key

        # Select a new node to start from, making sure we pick one
        # that still has valid outgoing edges
        last_node = graph.nodes.reject do |node|
          (node.entity.foreign_keys.each_value.map(&:entity) -
           graph.nodes.map(&:entity)).empty?
        end.sample
        break if last_node.nil?
      end

      graph
    end

    private

    # Produce a random where clause using fields along a given path
    # @return [String]
    def random_where_clause(path, count = 2)
      # Ensure we have at least one condition at the beginning of the path
      conditions = [path.entities.first.fields.values.sample]
      conditions += random_where_conditions path, count - 1

      return '' if conditions.empty?
      "WHERE #{conditions.map do |field|
        "#{path.find_field_parent(field).name}.#{field.name} = ?"
      end.join ' AND '}"
    end

    # Produce a random set of conditions for a where clause
    # @return [String]
    def random_where_conditions(path, count)
      1.upto(count).map do
        field = path.entities.sample.fields.values.sample
        next nil if field.name == '**'

        field
      end.compact
    end

    # Get the name to be used in the query for a condition field
    # @return [String]
    def condition_field_name(field, path)
      field_path = path.first.name
      path_end = path.index(field.parent)
      last_entity = path.first
      path[1..path_end].each do |entity|
        fk = last_entity.foreign_keys.values.find do |key|
          key.entity == entity
        end
        field_path += '.' + fk.name
        last_entity = entity
      end

      field_path
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
  # @return [Fixnum]
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
  # @return [Array<Fixnum>]
  def self.gaussian(mean, stddev)
    theta = 2 * Math::PI * rand
    rho = Math.sqrt(-2 * Math.log(1 - rand))
    scale = stddev * rho
    x = mean + scale * Math.cos(theta)
    y = mean + scale * Math.sin(theta)
    [x, y]
  end
end
