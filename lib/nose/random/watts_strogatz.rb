# frozen_string_literal: true

module NoSE
  module Random
    # Generates a random graph using the Watts-Strogatz model
    class WattsStrogatzNetwork < Network
      def initialize(params = {})
        super params

        @beta = params.fetch :beta, 0.5
        @node_degree = params.fetch :node_degree, 2
        @nodes = 0..(@nodes_nb - 1)

        @entities = @nodes.map do |node|
          create_entity node
        end

        build_initial_links
        rewire_links
        add_foreign_keys
      end

      private

      # Set up the initial links between all nodes
      # @return [void]
      def build_initial_links
        @nodes.each do |node|
          (@node_degree / 2).times do |i|
            add_link node, (node + i + 1) % @nodes_nb
          end
        end
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
    end
  end
end
