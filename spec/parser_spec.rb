module Sadvisor
  describe Statement do
    let(:workload) do
      Workload.new do
        Entity 'jane' do
          ID 'quux'
        end

        Entity 'foo' do
          ID 'bar'
          ForeignKey 'baz', 'jane'
          String 'bob'
        end
      end
    end

    subject(:statement) do
      Statement.new 'SELECT bob FROM foo WHERE ' \
                    'foo.bar = ? AND foo.baz > ? AND foo.baz.quux = ? ' \
                    'ORDER BY foo.baz LIMIT 5', workload
    end

    it 'reports the entity being selected from' do
      expect(statement.from).to eq workload['foo']
    end

    it 'knows its limits' do
      expect(statement.limit).to eq 5
    end

    it 'keeps a list of selected fields' do
      expect(statement.select).to match_array [workload['foo']['bob']]
    end

    it 'tracks the range field' do
      expect(statement.range_field).to eq workload['foo']['baz']
    end

    it 'tracks fields used in equality predicates' do
      expect(statement.eq_fields).to match_array [
        workload['foo']['bar'],
        workload['jane']['quux']
      ]
    end

    it 'can report the longest entity path' do
      expect(statement.longest_entity_path).to match_array [
        workload['foo'],
        workload['jane']
      ]
    end

    it 'can select all fields' do
      stmt = Statement.new 'SELECT * FROM foo', workload
      expect(stmt.select).to match_array workload['foo'].fields.values
    end
  end
end
