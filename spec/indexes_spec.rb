require_relative '../lib/indexes'
require_relative '../lib/parser'
require_relative '../lib/model'
require_relative '../lib/workload'

describe Index do
  before(:each) do
    @entity = Entity.new('Foo') * 100
    @id_field = IDField.new('Id')
    @field = IntegerField.new('Bar')
    @entity << @id_field
    @entity << @field

    @simple_query = Parser.parse('SELECT Id FROM Foo')
    @equality_query = Parser.parse('SELECT Id FROM Foo WHERE Foo.Id=3')
    @range_query = Parser.parse('SELECT Id FROM Foo WHERE Foo.Id > 3')
    @combo_query = Parser.parse('SELECT Id FROM Foo WHERE Foo.Id > 3 AND Foo.Bar = 1')

    @workload = Workload.new
    @workload.add_query @simple_query
    @workload.add_query @equality_query
    @workload.add_query @range_query
    @workload.add_query @range_query
    @workload.add_query @combo_query
    @workload.add_entity @entity
  end

  it 'has zero size when empty' do
    expect(Index.new([], []).fields.length).to eq(0)
    expect(Index.new([], []).entry_size).to eq(0)
    expect(Index.new([], []).size).to eq(0)
  end

  it 'contains fields' do
    index = Index.new([@id_field], [])
    expect(index.has_field? @id_field).to be_true
  end

  it 'can store additional fields' do
    index = Index.new([], [@id_field])
    expect(index.has_field? @id_field).to be_true
  end

  it 'can calculate its size' do
    index = Index.new([@id_field], [])
    @id_field *= 10
    expect(index.entry_size).to eq(@id_field.size)
    expect(index.size).to eq(@id_field.size * 10)
  end

  it 'does not support queries when empty' do
    index = Index.new([], [])
    expect(index.supports_query?(@simple_query, @workload)).to be_false
  end

  it 'supports equality queries on indexed fields' do
    index = Index.new([@id_field], [])
    expect(index.supports_query?(@equality_query, @workload)).to be_true
  end

  it 'does not support equality queries on unindexed fields' do
    index = Index.new([], [@id_field])
    expect(index.supports_query?(@equality_query, @workload)).to be_false
  end

  it 'supports range queries on indexed fields' do
    index = Index.new([@id_field], [])
    expect(index.supports_query?(@range_query, @workload)).to be_true
  end

  it 'supports range and equality predicates together in the correct order' do
    index = Index.new([@field, @id_field], [])
    expect(index.supports_query?(@combo_query, @workload)).to be_true
  end

  it 'does not support range and equality predicates in the wrong order' do
    index = Index.new([@id_field, @field], [])
    expect(index.supports_query?(@combo_query, @workload)).to be_false
  end

  it 'does not support range queries if the range field is not last' do
    index = Index.new([@id_field, @field], [])
    expect(index.supports_query?(@range_query, @workload)).to be_false
  end

  it 'can estimate the cost of evaluating a query' do
    index = Index.new([@id_field], [])
    expect(index.query_cost(@equality_query, @workload)).to eq(1600)
  end

  it 'can estimate the cost of evaluating a range query' do
    index = Index.new([@id_field], [])
    expect(index.query_cost(@range_query, @workload)).to be_within(0.001).of(1600.0/3)
  end
end
