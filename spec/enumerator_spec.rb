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
      query = Parser.parse 'SELECT Bar FROM Foo'
      indexes = enum.indexes_for_query query
      expect(indexes).to be_empty
    end

    it 'produces a simple index for a filter' do
      enum = IndexEnumerator.new @w
      query = Parser.parse 'SELECT Bar FROM Foo WHERE Foo.Baz=""'
      indexes = enum.indexes_for_query query
      expect(indexes).to have(1).item
      expect(indexes.to_a).to include \
        Index.new([@w['Foo']['Baz']], [@w['Foo']['Bar']], [])
    end

    it 'produces a simple index for a foreign key join' do
      enum = IndexEnumerator.new @w
      query = Parser.parse 'SELECT Baz FROM Bar WHERE Bar.Foo.Baz=""'
      indexes = enum.indexes_for_query query
      expect(indexes).to have(1).items
      expect(indexes.to_a).to include \
        Index.new([@w['Foo']['Baz']], [@w['Bar']['Baz']], [])
    end
  end
end
