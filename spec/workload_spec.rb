require_relative '../lib/workload'

describe Workload do
  before(:each) do
    @entity = Entity.new('Foo')
    @entity << IDField.new('Id')

    @valid_query = Parser.parse('SELECT Id FROM Foo')
  end

  it 'holds queries and entities' do
    workload = Workload.new

    workload.add_entity(@entity)
    expect(workload.entities.length).to eq(1)
    expect(workload.entities['Foo']).to be(@entity)

    workload.add_query(@valid_query)
    expect(workload.queries).to match_array([@valid_query])
    expect(workload.valid?).to be_true
  end

  it 'rejects queries with unknown entities' do
    workload = Workload.new
    workload.add_entity(@entity)

    query = Parser.parse('SELECT Id FROM Bar')
    workload.add_query(query)
    expect(workload.valid?).to be_false
  end

  it 'rejects queries which select unknown fields' do
    workload = Workload.new
    workload.add_entity(@entity)

    query = Parser.parse('SELECT Bar FROM Foo')
    workload.add_query(query)
    expect(workload.valid?).to be_false
  end

  it 'rejects queries with multiple range predicates' do
    workload = Workload.new
    workload.add_entity(@entity)
    query = Parser.parse('SELECT Id FROM Foo WHERE Foo.Id > 1 AND Foo.Id < 3')
    workload.add_query(query)
    expect(workload.valid?).to be_false
  end

  it 'rejects queries with unknown fields in where clauses' do
    workload = Workload.new
    workload.add_entity(@entity)
    query = Parser.parse('SELECT Id FROM Foo WHERE Foo.Bar = 1')
    workload.add_query(query)
    expect(workload.valid?).to be_false
  end
end
