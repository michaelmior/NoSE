module NoSE
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
          Integer 'quux'
        end
      end
    end

    subject(:query) do
      Query.new 'SELECT bob FROM foo.baz WHERE ' \
                'foo.bar = ? AND foo.baz > ? AND baz.quux = ? ' \
                'ORDER BY foo.baz LIMIT 5', workload
    end

    it 'reports the entity being selected from' do
      expect(query.from).to eq workload['foo']
    end

    it 'knows its limits' do
      expect(query.limit).to eq 5
    end

    it 'keeps a list of selected fields' do
      expect(query.select).to match_array [workload['foo']['bob']]
    end

    it 'tracks the range field' do
      expect(query.range_field).to eq workload['foo']['baz']
    end

    it 'tracks fields used in equality predicates' do
      expect(query.eq_fields).to match_array [
        workload['foo']['bar'],
        workload['jane']['quux']
      ]
    end

    it 'can report the longest entity path' do
      expect(query.longest_entity_path).to match_array [
        workload['foo'],
        workload['jane']
      ]
    end

    it 'can select all fields' do
      stmt = Query.new('SELECT * FROM foo WHERE foo.bar = ?', workload)
      expect(stmt.select).to match_array workload['foo'].fields.values
    end

    it 'compares equal regardless of constant values' do
      stmt1 = Query.new 'SELECT * FROM foo WHERE foo.quux = 3', workload
      stmt2 = Query.new 'SELECT * FROM foo WHERE foo.quux = 2', workload

      expect(stmt1).to eq stmt2
    end

    context 'when parsing literals' do
      it 'can find strings' do
        stmt = Query.new 'SELECT * FROM foo WHERE foo.bob = "ot"', workload
        expect(stmt.conditions.first.value).to eq 'ot'
      end

      it 'can find integers' do
        stmt = Query.new 'SELECT * FROM foo WHERE foo.quux = 3', workload
        expect(stmt.conditions.first.value).to eq 3
      end

      it 'fails if the value is the wrong type' do
        expect do
          Query.new 'SELECT * FROM foo WHERE foo.bob = 3', workload
        end.to raise_error TypeError
      end
    end
  end
end
