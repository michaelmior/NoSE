# frozen_string_literal: true

require 'set'

module NoSE
  # Representations for connected entities which are referenced in a {Query}
  module QueryGraph
    # A single node in a query graph
    class Node
      attr_reader :entity

      def initialize(entity)
        @entity = entity
      end

      # :nocov:
      def inspect
        @entity.name
      end
      # :nocov:

      # Two nodes are equal if they represent the same entity
      def ==(other)
        other.is_a?(Node) && @entity == other.entity
      end
      alias eql? ==

      def hash
        @entity.name.hash
      end
    end

    # An edge between two {Node}s in a {Graph}
    class Edge
      attr_reader :from, :to, :key

      def initialize(from, to, key)
        @from = from
        @to = to
        @key = key
      end

      # :nocov:
      def inspect
        @key.inspect
      end
      # :nocov:

      # Edges are equal if the canonical parameters used to construct
      # them are the same
      def ==(other)
        return false unless other.is_a? Edge
        canonical_params == other.canonical_params
      end
      alias eql? ==

      def hash
        canonical_params.hash
      end

      # Produce the parameters to initialize the canonical version of this edge
      # (this accounts for different directionality of edges)
      # @return [Array]
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
      attr_reader :nodes, :entities

      def initialize(nodes = [], *edges)
        @nodes = Set.new
        @entities = Set.new
        nodes.each { |n| add_node n }
        @edges = {}

        edges.each { |edge| add_edge(*edge) }
      end

      # :nocov:
      def inspect
        "Graph(nodes: #{@nodes.map(&:inspect).join(', ')}, " \
              "edges: #{@edges.inspect})"
      end
      # :nocov:

      # Graphs are equal if they have the same nodes and edges
      def ==(other)
        return false unless other.is_a? Graph
        @nodes == other.nodes && unique_edges == other.unique_edges
      end
      alias eql? ==

      def hash
        [@nodes, unique_edges.map(&:canonical_params).sort!].hash
      end

      # The total number of nodes in the graph
      # @return [Integer]
      def size
        @nodes.size
      end

      # Check if the graph is empty
      # @return [Boolean]
      def empty?
        @nodes.empty?
      end

      # Duplicate graphs ensuring that edges are correctly copied
      # @return [Graph]
      def dup
        new_graph = super

        new_graph.instance_variable_set :@nodes, @nodes.dup
        new_edges = Hash[@edges.map do |k, v|
          [k, v.dup]
        end]
        new_graph.instance_variable_set :@edges, new_edges
        new_graph.instance_variable_set :@unique_edges, nil

        new_graph
      end

      # Produce an array of entities in the desired join order
      # @return [Array<Entity>]
      def join_order(eq_fields)
        return [@nodes.first.entity] if @nodes.size == 1

        # Start with a leaf entity which has an equality predicate
        # and the lowest overall count of all such entities
        entities = @entities.dup
        leaf_entities = entities.select { |e| leaf_entity?(e) }
        join_order = [leaf_entities.select do |entity|
          eq_fields.map(&:parent).include?(entity)
        end.min_by(&:count)].compact
        join_order = [leaf_entities.min_by(&:count)] if join_order.empty?
        entities.delete join_order.first

        # Keep going until we have joined all entities
        until entities.empty?
          # Try to continue from the last entity
          next_entities = edges_for_entity(join_order.last).map do |edge|
            edge.to.entity
          end.to_set

          # Otherwise look for a new branch from the existing entities
          if (next_entities & entities).empty?
            next_entities = join_order.reduce(Set.new) do |edges, entity|
              edges.union(edges_for_entity(entity))
            end.map { |edge| edge.to.entity }.to_set
          end

          # Pick the entity with the smallest count, remove it, and keep going
          next_entity = (next_entities & entities).min_by(&:count)
          join_order << next_entity
          entities.delete next_entity
        end

        join_order
      end

      # Find the node corresponding to a given entity in the graph
      # @return [Node]
      def entity_node(entity)
        @nodes.find { |n| n.entity == entity }
      end

      # Check if the graph includes the given entity
      # @return [Boolean]
      def include_entity?(entity)
        !entity_node(entity).nil?
      end

      # Check if this entity is a leaf in the graph (at most one edge)
      # @return [Boolean]
      def leaf_entity?(entity)
        node = entity_node(entity)
        return false if node.nil?
        !@edges.key?(node) || @edges[node].size <= 1
      end

      # Produce a path in the graph between two nodes
      # @return [KeyPath]
      def path_between(node1, node2)
        node1 = find_entity_node node1
        node2 = find_entity_node node2
        keys = path_between_visit [node1.entity.id_field], [node1], node2

        KeyPath.new keys
      end

      # Find the node which corresponds to a given entity
      def find_entity_node(entity)
        @nodes.find { |n| n.entity == entity }
      end

      # Add a new node to the graph
      # @return [Node] the new node which was added
      def add_node(node)
        if node.is_a? Entity
          existing = find_entity_node node
          if existing.nil?
            node = Node.new(node)
            @nodes.add node
            @entities.add node.entity
          else
            node = existing
          end
        elsif !@nodes.include? node
          @nodes.add node
          @entities.add node.entity
        end

        node
      end

      # Add a new edge betwene two nodes in the graph
      # @return [void]
      def add_edge(node1, node2, key)
        node1 = add_node node1
        node2 = add_node node2

        @edges[node1] = Set.new unless @edges.key? node1
        @edges[node1].add Edge.new(node1, node2, key)
        @edges[node2] = Set.new unless @edges.key? node2
        @edges[node2].add Edge.new(node2, node1, key.reverse)

        @unique_edges = nil
      end

      # Prune nodes not reachable from a given starting node
      # @return [void]
      def prune(start)
        to_visit = [start]
        reachable = Set.new([start])

        # Determine which nodes are reachable
        until to_visit.empty?
          discovered = Set.new
          to_visit.each do |node|
            next unless @edges.key? node
            discovered += @edges[node].map(&:to).to_set
          end
          to_visit = discovered - reachable
          reachable += discovered
        end

        remove_nodes @nodes - reachable
      end

      # Remove nodes (or entities) from the graph
      def remove_nodes(nodes)
        # Find all the nodes to be removed if needed
        nodes.map! { |n| n.is_a?(Node) ? n : find_entity_node(n) }

        # Remove any nodes and edges which are not reachable
        @edges.reject! { |node| nodes.include? node }
        @edges.each do |_, edges|
          edges.reject! do |edge|
            nodes.include?(edge.to) || nodes.include?(edge.from)
          end
        end
        @edges.reject! { |_, edges| edges.empty? }
        @nodes -= nodes.to_set
        @entities -= nodes.map(&:entity)

        @unique_edges = nil
      end

      # Construct a list of all unique edges in the graph
      # @reutrn [Array<Edge>]
      def unique_edges
        # We memoize this calculation so check if it has already been computed
        return @unique_edges unless @unique_edges.nil?

        all_edges = @edges.values.reduce(&:union).to_a
        all_edges.uniq!(&:canonical_params)

        @unique_edges = all_edges.to_set
      end

      # Produce an enumerator which yields all subgraphs of this graph
      # @return [Set<Graph>]
      def subgraphs(recursive = true)
        # We have no subgraphs if there is only one node
        return [self] if @nodes.size == 1

        all_edges = unique_edges
        all_subgraphs = Set.new([self])
        all_edges.each do |remove_edge|
          # Construct new graphs from either side of the cut edge
          graph1 = Graph.new [remove_edge.from]
          graph2 = Graph.new [remove_edge.to]
          all_edges.each do |edge|
            next if edge == remove_edge

            graph1.add_edge edge.from, edge.to, edge.key
            graph2.add_edge edge.from, edge.to, edge.key
          end

          # Prune the graphs before yielding them and their subgraphs
          graph1.prune remove_edge.from
          all_subgraphs.add graph1
          all_subgraphs += graph1.subgraphs if recursive

          graph2.prune remove_edge.to
          all_subgraphs.add graph2
          all_subgraphs += graph2.subgraphs if recursive
        end

        all_subgraphs.to_set
      end

      # Construct a graph from a KeyPath
      # @return [Graph]
      def self.from_path(path)
        return Graph.new if path.empty?

        path = path.entries if path.is_a?(KeyPath)
        graph = Graph.new
        prev_node = graph.add_node path.first.parent
        path[1..-1].each do |key|
          next_node = graph.add_node key.entity
          graph.add_edge prev_node, next_node, key
          prev_node = next_node
        end

        graph
      end

      # Convert this graph into a path if possible
      # @return [KeyPath]
      # @raise [InvalidPathException]
      def to_path(start_entity)
        return KeyPath.new if @nodes.empty?

        start = @nodes.find { |n| n.entity == start_entity }

        fail InvalidPathException, 'Need start for path conversion' \
          if start.nil?
        keys = [start.entity.id_field]
        entities = Set.new [start.entity]

        edges = edges_for_entity start.entity
        until edges.empty?
          new_entities = edges.map { |e| e.to.entity }.to_set.delete_if do |n|
            entities.include?(n)
          end
          break if new_entities.empty?
          fail InvalidPathException, 'Graph cannot be converted to path' \
            if new_entities.size > 1
          edge = edges.find { |e| !entities.include? e.to.entity }
          keys << edge.key
          entities.add edge.to.entity
          edges = edges_for_entity edge.to.entity
        end

        KeyPath.new keys
      end

      # Produce a path through the graph of maximum length
      # @return [KeyPath]
      def longest_path
        return KeyPath.new [@nodes.first.entity.id_field] if @nodes.size == 1

        longest_path = []
        @nodes.each do |node|
          next unless leaf_entity?(node.entity)

          longest_path = longest_path_visit node, Set.new([node]), [],
                                            longest_path
        end

        KeyPath.new [longest_path.first.from.entity.id_field] +
                    longest_path.map(&:key)
      end

      # Output an image of the query graph
      # @return [void]
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

      # Split this graph into multiple graphs at the given
      # entity, optionally removing the corresponding node
      # return [Array<Graph>]
      def split(entity, keep = false)
        # Simple case with one node
        return keep ? [dup] : [] if size == 1

        # Find the node corresponding to the entity to remove
        remove_node = @nodes.find { |n| n.entity == entity }

        # For each edge from this entity, build a new graph with
        # the entity removed and explore the different paths
        @edges[remove_node].map do |edge|
          new_graph = dup
          remove_nodes = (@edges[remove_node] - [edge]).map(&:to)
          remove_nodes << remove_node unless keep
          new_graph.remove_nodes remove_nodes
          new_graph.prune edge.to

          new_graph
        end
      end

      # Produce the keys for all edges leaving the given entity
      # @return [Array<Fields::ForeignKeyField>]
      def keys_from_entity(entity)
        edges_for_entity(entity).map(&:key)
      end

      private

      # Helper to find the longest path in the graph starting at a given
      # node and not exploring nodes which have been visited
      # We keep track of the visited edges and update the longest path
      # @return [Array<Edge>]
      def longest_path_visit(node, visited_nodes, edges, longest_path)
        # Find new edges we may want to traverse
        new_edges = @edges[node].reject { |e| visited_nodes.include? e.to }

        # If we reached the end of a path, see if we found a longer one
        if new_edges.empty?
          return edges.length > longest_path.length ? edges : longest_path
        end

        new_edges.each do |edge|
          longest_path = longest_path_visit edge.to,
                                            visited_nodes.dup.add(edge.to),
                                            edges.dup << edge, longest_path
        end

        longest_path
      end

      # Return all the edges starting at a given entity
      # @return [Array<Edge>]
      def edges_for_entity(entity)
        pair = @edges.find { |n, _| n.entity == entity }
        pair.nil? ? Set.new : pair.last
      end

      # Helper for path_between to recursively visit nodes in the graph
      # @return [void]
      def path_between_visit(keys, nodes, end_node)
        return keys if nodes.last == end_node
        return nil unless @edges.key? nodes.last

        @edges[nodes.last].lazy.map do |edge|
          next if nodes.include? edge.to
          path_between_visit keys.dup << edge.key,
                             nodes.dup << edge.to, end_node
        end.reject(&:nil?).first
      end
    end

    # Thrown when trying to convert a graph which is not a path
    class InvalidPathException < StandardError
    end
  end
end
