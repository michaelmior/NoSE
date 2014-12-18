module NoSE
  describe IndexEnumerator do
    subject(:enum) { IndexEnumerator.new workload }
    let(:workload) do
      Workload.new do
        Entity 'Foo' do
          ID 'Bar'
          String 'Baz', 10
        end

        Entity 'Bar' do
          ID 'Baz'
          ForeignKey 'Quux', 'Foo'
        end
      end
    end

    it 'produces a simple index for a filter' do
      query = Statement.new 'SELECT Bar FROM Foo WHERE Foo.Baz = ?', workload
      indexes = enum.indexes_for_query query

      expect(indexes.to_a).to include \
        Index.new([workload['Foo']['Baz']], [], [workload['Foo']['Bar']],
                  [workload['Foo']])
    end

    it 'produces a simple index for a foreign key join' do
      query = Statement.new 'SELECT Baz FROM Bar WHERE Bar.Quux.Baz = ?',
                            workload
      indexes = enum.indexes_for_query query

      expect(indexes).to include \
        Index.new([workload['Foo']['Baz']], [], [workload['Bar']['Baz']],
                  [workload['Foo'], workload['Bar']])
    end

    it 'produces a simple index for a filter within a workload' do
      query = Statement.new 'SELECT Bar FROM Foo WHERE Foo.Baz = ?', workload
      workload.add_query query
      indexes = enum.indexes_for_workload

      expect(indexes.to_a).to include \
        Index.new([workload['Foo']['Baz']], [], [workload['Foo']['Bar']],
                  [workload['Foo']])
    end

    it 'does not produce empty indexes' do
      query = Statement.new 'SELECT Baz FROM Bar WHERE Bar.Quux.Baz = ?',
                            workload
      workload.add_query query
      indexes = enum.indexes_for_workload
      expect(indexes).to all(satisfy do |index|
        !index.order_fields.empty? || !index.extra.empty?
      end)
    end

    it 'produces indices with multiple entities in the path' do
      query = Statement.new 'SELECT Baz FROM Bar ' \
                            'WHERE Bar.Quux.Baz = ?', workload
      workload.add_query query
      indexes = enum.indexes_for_workload
      expect(indexes).to include Index.new([workload['Foo']['Baz']], [],
                                           [workload['Bar']['Baz']],
                                           [workload['Foo'], workload['Bar']])
    end
  end
end
