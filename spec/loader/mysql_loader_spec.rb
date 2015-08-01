require 'nose/loader/mysql'

module NoSE::Loader
  describe MysqlLoader do
    # Mock the client of a loader to return canned responses to SQL queries
    def mock_loader(responses, count)
      loader = MysqlLoader.new

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
      # Simple Array subclass so we can use .each(as: :array)
      class EachArray < Array
        def each(*args, **options)
          super()
        end
      end

      loader = mock_loader({
        'SHOW TABLES' => EachArray.new([['Foo']]),
        'SELECT COUNT(*) FROM Foo' => [{ 'COUNT()*)' => 10 }],
        'DESCRIBE Foo' => EachArray.new([
          ['FooId', 'int(10) unsigned', 'NO', 'PRI', 'NULL', ''],
          ['Bar', 'int(10) unsigned', 'NO', '', 'NULL', ''],
          ['Baz', 'float', 'NO', '', 'NULL', ''],
          ['Quux', 'datetime', 'NO', '', 'NULL', ''],
          ['Corge', 'text', 'NO', '', 'NULL', ''],
          ['Garply', 'varchar(10)', 'NO', '', 'NULL', '']
        ])
      }, 3)

      workload = loader.workload({})
      expect(workload.model.entities).to have(1).item

      entity = workload.model.entities.values.first
      expect(entity.name).to eq 'Foo'
      expect(entity.fields).to have(6).items

      expect(entity.fields.values[0]).to be_a NoSE::Fields::IDField
      expect(entity.fields.values[1]).to be_a NoSE::Fields::IntegerField
      expect(entity.fields.values[2]).to be_a NoSE::Fields::FloatField
      expect(entity.fields.values[3]).to be_a NoSE::Fields::DateField
      expect(entity.fields.values[4]).to be_a NoSE::Fields::StringField
      expect(entity.fields.values[5]).to be_a NoSE::Fields::StringField
    end
  end
end
