require 'set'

module NoSE
  module QueryGraph
    # A single node in a query graph
    class GraphNode
      attr_reader :entity

      def initialize(entity)
        @entity = entity
      end
    end

    # A graph identifying entities and relationships involved in a query
    class Graph
      def initialize
        @nodes = Set.new
        @edges = Hash.new { |h, k| h[k] = Set.new }
      end

      # Add a new node to the graph
      def add_node(entity)
        node = GraphNode.new entity
        @nodes.add node
        node
      end

      # Add a new edge betwene two nodes in the graph
      def add_edge(node1, node2, key)
        @nodes.add node1
        @edges[node1].add key

        @nodes.add node2
        @edges[node2].add key.reverse
      end
    end
  end
end
