module NoSE
  module Random
    shared_examples 'a network' do
      it 'has a default of 10 entities' do
        expect(network.entities).to have(10).items
      end

      it 'generates entities with IDs' do
        expect(network.entities).to all(satisfy do |entity|
          entity.fields.values.any? { |field| field.is_a? Fields::IDField }
        end)
      end

      it 'does not generate disconnected entities' do
        expect(network.entities).to all(satisfy do |entity|
          connected = !entity.foreign_keys.empty?
          connected ||= network.entities.any? do |other|
            other.foreign_keys.each_value.map(&:entity).include? entity
          end

          connected
        end)
      end
    end

    describe BarbasiAlbertNetwork do
      let(:network) { BarbasiAlbertNetwork.new }
      it_behaves_like 'a network'
    end

    describe WattsStrogatzNetwork do
      let(:network) { BarbasiAlbertNetwork.new }
      it_behaves_like 'a network'
    end

    describe StatementGenerator do
      include_context 'entities'

      subject(:sgen) { StatementGenerator.new(workload.model) }

      before(:each) { ::Random.srand 0 }

      it 'generates valid insertions' do
        expect(sgen.random_insert).to be_a Insert
      end

      it 'generates valid updates' do
        expect(sgen.random_update).to be_a Update
      end

      it 'generates valid deletions' do
        expect(sgen.random_delete).to be_a Delete
      end

      it 'generates valid queries' do
        expect(sgen.random_query).to be_a Query
      end

      it 'generates random graphs' do
        expect(sgen.random_graph(4)).to be_a QueryGraph::Graph
      end
    end
  end
end
