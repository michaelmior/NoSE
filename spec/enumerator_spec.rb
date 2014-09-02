module Sadvisor
  describe IndexEnumerator do
    before(:each) do
      @w = w = Workload.new

      @w << Entity.new('Foo') do
        ID 'Bar'
        String 'Baz', 10
      end

      @w << Entity.new('Bar') do
        ID 'Baz'
        ForeignKey 'Quux', w['Foo']
      end

    end

    it 'produces no indices for a simple select' do
      enum = IndexEnumerator.new @w
      query = Statement.new 'SELECT Bar FROM Foo', @w
      indexes = enum.indexes_for_query query
      expect(indexes).to be_empty
    end

    it 'produces a simple index for a filter' do
      enum = IndexEnumerator.new @w
      query = Statement.new 'SELECT Bar FROM Foo WHERE Foo.Baz = ?', @w
      indexes = enum.indexes_for_query query
      expect(indexes).to have(1).item
      expect(indexes.to_a).to include \
        Index.new([@w['Foo']['Baz']], [], [@w['Foo']['Bar']], [@w['Foo']])
    end

    it 'produces a simple index for a foreign key join' do
      enum = IndexEnumerator.new @w
      query = Statement.new 'SELECT Baz FROM Bar WHERE Bar.Quux.Baz = ?', @w
      indexes = enum.indexes_for_query query
      expect(indexes).to include \
        Index.new([@w['Foo']['Baz']], [], [@w['Bar']['Baz']], [@w['Bar'], @w['Foo']])
    end

    it 'produces a simple index for a filter within a workload' do
      enum = IndexEnumerator.new @w
      query = Statement.new 'SELECT Bar FROM Foo WHERE Foo.Baz = ?', @w
      @w.add_query query
      indexes = enum.indexes_for_workload
      expect(indexes).to have(1).item
      expect(indexes.to_a).to include \
        Index.new([@w['Foo']['Baz']], [], [@w['Foo']['Bar']], [@w['Foo']])
    end

    it 'does not produce empty indexes' do
      enum = IndexEnumerator.new @w
      query = Statement.new 'SELECT Baz FROM Bar WHERE Bar.Quux.Baz = ?', @w
      @w.add_query query
      indexes = enum.indexes_for_workload
      expect(indexes).to all(satisfy do |index|
        !index.order_fields.empty? || !index.extra.empty?
      end)
    end
  end
end
