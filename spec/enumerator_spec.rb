describe Sadvisor::IndexEnumerator do
  subject { Sadvisor::IndexEnumerator }

  before(:each) do
    @w = Sadvisor::Workload.new

    @w << Sadvisor::Entity.new('Foo') do
      ID 'Bar'
      String 'Baz', 10
    end

    @query = Sadvisor::Parser.parse 'SELECT Bar FROM Foo'
  end

  it 'can produce all possible indices for an entity' do
    indexes = subject.indexes_for_entity(@w['Foo']).to_a
    expect(indexes).to match_array [
      Sadvisor::Index.new([@w['Foo']['Bar']], []),
      Sadvisor::Index.new([@w['Foo']['Baz']], []),
      Sadvisor::Index.new([@w['Foo']['Bar'], @w['Foo']['Baz']], []),
      Sadvisor::Index.new([@w['Foo']['Baz'], @w['Foo']['Bar']], []),
      Sadvisor::Index.new([@w['Foo']['Bar']], [@w['Foo']['Baz']]),
      Sadvisor::Index.new([@w['Foo']['Baz']], [@w['Foo']['Bar']])
    ]
  end

  it 'can produce all possible indices for a query' do
    indexes = subject.indexes_for_query(@query, @w).to_a
    expect(indexes).to match_array [
      Sadvisor::Index.new([@w['Foo']['Bar']], [])
    ]
  end
end
