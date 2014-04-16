describe IndexEnumerator do
  before(:each) do
    @w = Workload.new

    @w << Entity.new('Foo') do
      ID 'Bar'
      String 'Baz', 10
    end

    @query = Parser.parse 'SELECT Bar FROM Foo'
  end

  it 'can produce all possible indices for an entity' do
    indexes = IndexEnumerator.indexes_for_entity(@w['Foo']).to_a
    expect(indexes).to match_array [
      Index.new([@w['Foo']['Bar']], []),
      Index.new([@w['Foo']['Baz']], []),
      Index.new([@w['Foo']['Bar'], @w['Foo']['Baz']], []),
      Index.new([@w['Foo']['Baz'], @w['Foo']['Bar']], []),
      Index.new([@w['Foo']['Bar']], [@w['Foo']['Baz']]),
      Index.new([@w['Foo']['Baz']], [@w['Foo']['Bar']])
    ]
  end

  it 'can produce all possible indices for a query' do
    indexes = IndexEnumerator.indexes_for_query(@query, @w).to_a
    expect(indexes).to match_array [
      Index.new([@w['Foo']['Bar']], []),
    ]
  end
end
