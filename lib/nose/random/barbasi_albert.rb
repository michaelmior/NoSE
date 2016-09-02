# frozen_string_literal: true

module NoSE
  module Random
    # Generates a random graph using the Barbasi-Albert model
    class BarbasiAlbertNetwork < Network
      def initialize(params = {})
        super params

        # We assume for now that m0 = m = 2

        create_entities
        add_foreign_keys
      end

      private

      # Create all the entities in the graph and their connections
      # @return [void]
      def create_entities
        # Add the initial one or two entities
        @entities = [create_entity(0)]
        return if @nodes_nb == 1

        @entities << create_entity(1)
        add_link 0, 1
        return if @nodes_nb == 2

        @entities << create_entity(2)
        add_link 2, 0
        add_link 2, 1
        return if @nodes_nb == 3

        # Add and connect more entities as needed
        3.upto(@nodes_nb - 1).each do |node|
          @entities << create_entity(node)
          pick = Pickup.new(0.upto(node - 1).to_a,
                            key_func: ->(n) { n },
                            weight_func: ->(n) { @neighbours[n].size },
                            uniq: true)
          pick.pick(2).each do |node2|
            add_link node, node2
          end
        end
      end
    end
  end
end
