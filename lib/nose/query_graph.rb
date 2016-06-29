require 'set'

module NoSE
  module QueryGraph
    # A single node in a query graph
    class Node
      attr_reader :entity

      def initialize(entity)
        @entity = entity
      end

      def inspect
        @entity.name
      end

      # Two nodes are equal if they represent the same entity
      def ==(other)
        other.is_a?(Node) && @entity == other.entity
      end
      alias eql? ==

      def hash
        @entity.name.hash
      end
    end

    class Edge
      attr_reader :from, :to, :key

      def initialize(from, to, key)
        @from = from
        @to = to
        @key = key
      end

      def inspect
        @key.inspect
      end

      def ==(other)
        return false unless other.is_a? Edge
        canonical_params == other.canonical_params
      end
      alias eql? ==

      def hash
        canonical_params.hash
      end

      # Produce the parameters to initialize the canonical version of this edge
      def canonical_params
        if @from.entity.name > @to.entity.name
          [@from.entity.name, @to.entity.name, @key.name]
        else
          [@to.entity.name, @from.entity.name, @key.reverse.name]
        end
      end
    end

    # A graph identifying entities and relationships involved in a query
    class Graph
      attr_reader :root, :nodes

      def initialize(root = nil, *edges)
        @nodes = Set.new
        @root = add_node root unless root.nil?
        @edges = Hash.new { |h, k| h[k] = Set.new }

        edges.each { |edge| add_edge(*edge) }
      end

      def inspect
        "Graph(root: #{@root.inspect}, " \
              "nodes: #{@nodes.map(&:inspect).join(', ')}, " \
              "edges: #{@edges.inspect})"
      end

      def ==(other)
        return false unless other.is_a? Graph
        @root == other.root && @nodes == other.nodes &&
          @edges = other.instance_variable_get(:@edges)
      end
      alias eql? ==

      def hash
        [@root, @nodes, @edges].hash
      end

      # Produce all entities contained in this graph
      def entities
        @nodes.map(&:entity).to_set
      end

      # Add a new node to the graph
      def add_node(node)
        node = Node.new node if node.is_a? Entity
        @nodes.add node
        node
      end

      # Add a new edge betwene two nodes in the graph
      def add_edge(node1, node2, key)
        node1 = add_node node1
        node2 = add_node node2

        @edges[node1].add Edge.new(node1, node2, key)
        @edges[node2].add Edge.new(node2, node1, key.reverse)
      end

      # Prune nodes not reachable from the root
      def prune
        fail 'Cannot prune without root' if @root.nil?
        to_visit = [@root]
        reachable = Set.new([@root])

        # Determine which nodes are reachable from the root
        until to_visit.empty?
          discovered = Set.new
          to_visit.each do |node|
            discovered += @edges[node].map(&:to).to_set
          end
          to_visit = discovered - reachable
          reachable += discovered
        end

        # Remove any nodes and edges which are not reachable
        @edges.select! { |node| reachable.include? node }
        @edges.each do |_, edges|
          edges.select! do |edge|
            reachable.include?(edge.to) && reachable.include?(edge.from)
          end
        end
        @edges.reject! { |_, edges| edges.empty? }
        @nodes = reachable
      end

      # Produce an enumerator which yields all subgraphs of this graph
      def subgraphs(recursive = true)
        # We have no subgraphs if there is only one node
        return [] if @nodes.size == 1

        # Construct a list of all unique edges in the graph
        all_edges = @edges.values.reduce(&:union).to_a.uniq(&:canonical_params)

        all_subgraphs = Set.new
        all_edges.each do |remove_edge|
          # Construct new graphs rooted at either side of the cut edge
          graph1 = Graph.new(remove_edge.from)
          graph2 = Graph.new(remove_edge.to)
          (all_edges - [remove_edge]).each do |edge|
            graph1.add_edge edge.from, edge.to, edge.key
            graph2.add_edge edge.from, edge.to, edge.key
          end

          # Prune the graphs before yielding them and their subgraphs
          graph1.prune
          all_subgraphs.add graph1
          all_subgraphs += graph1.subgraphs if recursive

          graph2.prune
          all_subgraphs.add graph2
          all_subgraphs += graph2.subgraphs if recursive
        end

        all_subgraphs.to_set
      end

      # Convert this graph into a path if possible
      def to_path
        fail 'Need root for path conversion' if @root.nil?
        keys = [@root.entity.id_fields.first]
        entities = Set.new [@root.entity]

        edges = edges_for_entity keys.last.parent
        until edges.empty?
          break if (edges.map { |e| e.to.entity }.to_set - entities).empty?
          fail 'Graph cannot be converted to path' if edges.size > 1
          keys << edges.first.key
          entities.add edges.first.to.entity
          edges = edges_for_entity keys.last.parent
        end

        KeyPath.new keys
      end

      # Output an image of the query graph
      def output(format, filename)
        graph = GraphViz.new :G, type: :graph
        nodes = Hash[@nodes.map do |node|
          [node, graph.add_nodes(node.entity.name)]
        end]

        @edges.each do |_, edges|
          edges.each do |edge|
            graph.add_edges nodes[edge.from], nodes[edge.to],
                            label: edge.key.name
          end
        end

        graph.output(**{ format => filename })
      end

      private

      # Return all the edges starting at a given entity
      def edges_for_entity(entity)
        @edges.find { |n, _| n.entity == entity}.last
      end
    end
  end
end
