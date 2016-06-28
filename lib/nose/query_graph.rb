require 'set'

module NoSE
  module QueryGraph
    # A single node in a query graph
    class Node
      attr_reader :entity

      def initialize(entity)
        @entity = entity
      end
    end

    class Edge
      attr_reader :from, :to, :key

      def initialize(from, to, key)
        @from = from
        @to = to
        @key = key
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
        node = Node.new entity
        @nodes.add node
        node
      end

      # Add a new edge betwene two nodes in the graph
      def add_edge(node1, node2, key)
        @nodes.add node1
        @edges[node1].add Edge.new(node1, node2, key)

        @nodes.add node2
        @edges[node2].add Edge.new(node2, node1, key.reverse)
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
    end
  end
end
