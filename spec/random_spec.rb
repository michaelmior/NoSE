module NoSE
  describe Network do
    subject(:network) { Network.new }

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
        connected = entity.fields.values.any? do |field|
          field.is_a? Fields::ForeignKeyField
        end ||
        connected ||= network.entities.any? do |other|
          !other.foreign_key_for(entity).nil?
        end
      end)
    end
  end

  describe QueryGenerator do
    let(:workload) do
      Workload.new do
        Entity 'foo' do
          ID 'ID'
        end
      end
    end
    subject(:qgen) { QueryGenerator.new(workload.model) }

    it 'generates valid queries' do
      expect(qgen.random_query).to be_a Query
    end
  end
end
