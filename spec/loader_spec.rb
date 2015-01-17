require 'nose/loader/csv'
require 'nose/loader/mysql'

require 'fakefs/spec_helpers'

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

  describe CsvLoader do
    include_context 'entities'
    include FakeFS::SpecHelpers

    before(:each) do
      FileUtils.mkdir_p '/tmp/csv'

      File.open '/tmp/csv/User.csv', 'w' do |file|
        file.puts <<-EOF.gsub(/^ {8}/, '')
        UserId,Username,City
        1,Alice,Chicago
        EOF
      end
    end

    it 'can load data into a backend' do
      backend = instance_spy NoSE::Backend::BackendBase

      index = NoSE::Index.new [user['City']], [], [user['Username']], [user]
      loader = CsvLoader.new workload, backend
      loader.load([index], {directory: '/tmp/csv'})

      expect(backend).to have_received(:index_insert_chunk).with(
        index,
        [{'User_UserId' => '1', 'User_Username' => 'Alice', 'User_City' => 'Chicago'}])
    end
  end
end
