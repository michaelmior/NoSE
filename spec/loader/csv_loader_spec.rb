require 'nose/loader/csv'

module NoSE
  module Loader
    describe CsvLoader do
      include_context 'entities'
      include FakeFS::SpecHelpers

      before(:each) do
        FileUtils.mkdir_p '/tmp/csv'

        File.open '/tmp/csv/User.csv', 'w' do |file|
          file.puts <<-EOF.gsub(/^ {10}/, '')
            UserId,Username,City
            1,Alice,Chicago
          EOF
        end
      end

      it 'can load data into a backend' do
        backend = instance_spy Backend::Backend
        allow(backend).to receive(:by_id_graph).and_return(false)

        index = Index.new [user['City']], [user['UserId']],
                          [user['Username']],
                          QueryGraph::Graph.from_path([user.id_field])
        loader = CsvLoader.new workload, backend
        loader.load([index], directory: '/tmp/csv')

        expect(backend).to have_received(:index_insert_chunk).with(
          index,
          [{
            'User_UserId' => '1',
            'User_Username' => 'Alice',
            'User_City' => 'Chicago'
          }]
        )
      end
    end
  end
end
