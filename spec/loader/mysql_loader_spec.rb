require 'nose/loader/mysql'

module NoSE
  module Loader
    describe MysqlLoader do
      # Mock the client of a loader to return canned responses to SQL queries
      def mock_loader(responses, count)
        loader = MysqlLoader.new

        allow(loader).to receive(:new_client) do
          client = double('client')
          expect(client).to receive(:query) do |query|
            responses.each_pair.find { |k, _| k == query }.last
          end.exactly(count).times

          client
        end

        loader
      end

      it 'can generate a workload from a database' do
        # Simple Array subclass so we can use .each(as: :array)
        class EachArray < Array
          def each(*_args, **_options)
            super()
          end
        end

        loader = mock_loader(
          {
            'SHOW TABLES' => EachArray.new([['Foo']]),
            'SELECT COUNT(*) FROM Foo' => [{ 'COUNT()*)' => 10 }],
            'DESCRIBE Foo' => EachArray.new(
              [
                ['FooId', 'int(10) unsigned', 'NO', 'PRI', 'NULL', ''],
                ['Bar', 'int(10) unsigned', 'NO', '', 'NULL', ''],
                ['Baz', 'float', 'NO', '', 'NULL', ''],
                ['Quux', 'datetime', 'NO', '', 'NULL', ''],
                ['Corge', 'text', 'NO', '', 'NULL', ''],
                ['Garply', 'varchar(10)', 'NO', '', 'NULL', '']
              ]
            )
          }, 3
        )

        workload = loader.workload({})
        expect(workload.model.entities).to have(1).item

        entity = workload.model.entities.values.first
        expect(entity.name).to eq 'Foo'
        expect(entity.fields).to have(6).items

        expect(entity.fields.values[0]).to be_a Fields::IDField
        expect(entity.fields.values[1]).to be_a Fields::IntegerField
        expect(entity.fields.values[2]).to be_a Fields::FloatField
        expect(entity.fields.values[3]).to be_a Fields::DateField
        expect(entity.fields.values[4]).to be_a Fields::StringField
        expect(entity.fields.values[5]).to be_a Fields::StringField
      end

      context 'when loading into a backend', mysql: true do
        let(:workload) { Workload.load 'rubis' }
        let(:backend) do
          dummy = double('backend')
          allow(dummy).to receive(:by_id_graph).and_return(false)
          allow(dummy).to receive(:index_empty?).and_return(true)

          dummy
        end

        let(:config) do
          {
            host: '127.0.0.1',
            username: 'root',
            database: 'nose'
          }
        end

        let(:loader) do
          MysqlLoader.new workload, backend
        end

        it 'can load a simple ID index', mysql: true do
          user = workload.model['users']
          index = Index.new [user['id']], [], [user['nickname']],
                            QueryGraph::Graph.from_path([user['id']])
          expect(backend).to receive(:index_insert_chunk).with(
            index, [
              {
                'users_id' => 2,
                'users_nickname' => '08ec962a-fc56-40a3-9e07-1fca0520253c'
              }
            ]
          )
          loader.load([index], config, false, 1)
        end

        it 'can load an index across multiple entities', mysql: true do
          user = workload.model['users']
          item = workload.model['items']
          index = Index.new [user['id']], [item['id']], [item['name']],
                            QueryGraph::Graph.from_path(
                              [user['id'], user['items_sold']]
                            )
          expect(backend).to receive(:index_insert_chunk).with(
            index, [
              {
                'users_id' => 1,
                'items_id' => 45,
                'items_name' => 'repellat alias consequatur'
              }
            ]
          )
          loader.load([index], config, false, 1)
        end
      end
    end
  end
end
