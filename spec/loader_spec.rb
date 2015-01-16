require 'nose/loaders/mysql'

module NoSE
  describe MySQLLoader do
    # Mock the client of a loader to return canned responses to SQL queries
    def mock_loader(responses, count)
      loader = MySQLLoader.new

      allow(loader).to receive(:new_client) do
        client = double('client')
        expect(client).to receive(:query) do |query|
          responses.each_pair.find { |k, v| k == query }.last
        end.exactly(count).times

        client
      end

      loader
    end

    it 'can generate a workload from a database' do
      loader = mock_loader({
        'SHOW TABLES' => [['Foo']],
        'SELECT COUNT(*) FROM Foo' => [[10]],
        'DESCRIBE Foo' => [
          ['FooId', 'int(10) unsigned', 'NO', 'PRI', 'NULL', ''],
          ['Bar', 'int(10) unsigned', 'NO', '', 'NULL', '']
        ]
      }, 3)

      workload = loader.workload({})
      expect(workload.model.entities).to have(1).item

      entity = workload.model.entities.values.first
      expect(entity.name).to eq 'Foo'
      expect(entity.fields).to have(2).items
    end
  end
end
