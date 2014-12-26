require 'nose/loaders/mysql'

module NoSE
  describe MySQLLoader do
    it 'can generate a workload from a database' do
      loader = MySQLLoader.new

      # Mock SQL query responses
      allow(loader).to receive(:new_client) do
        client = double('client')
        expect(client).to receive(:query) do |query|
          case query
          when 'SHOW TABLES'
            [['Foo']]
          when 'SELECT COUNT(*) FROM Foo'
            [[10]]
          when 'DESCRIBE Foo'
            # Field, Type, Null, Key, Default, Extra
            [['FooId', 'int(10) unsigned', 'NO', 'PRI', 'NULL', ''],
             ['Bar', 'int(10) unsigned', 'NO', '', 'NULL', '']]
          else
            [[]]
          end
        end.exactly(3).times

        client
      end

      workload = loader.workload({})
      expect(workload.entities).to have(1).item

      entity = workload.entities.values.first
      expect(entity.name).to eq 'Foo'
      expect(entity.fields).to have(2).items
    end
  end
end
