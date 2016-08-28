module NoSE
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

      # Add and connect more entities as needed
      edges = 2
      2.upto(@nodes_nb - 1).each do |node|
        @entities << create_entity(node)
        pick = Pickup.new(0.upto(node - 1).to_a,
                          weight_func: ->(x) { x },
                          uniq: true) { |n| @neighbours[n].size }
        pick.pick(2).each do |node2|
          add_link node, node2
          edges += 2
        end
      end
    end
  end
end
