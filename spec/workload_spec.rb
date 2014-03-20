require 'workload'

describe Workload do
  let(:entity)      { Entity.new('Foo') << field }
  let(:field)       { IDField.new('Id') }
  let(:valid_query) { Parser.parse('SELECT Id FROM Foo') }

  it 'holds queries and entities' do
    subject.add_entity(entity)
    expect(subject.entities.length).to eq 1
    expect(subject.entities['Foo']).to be entity

    subject.add_query(valid_query)
    expect(subject.queries).to match_array [valid_query]
    expect(subject.valid?).to be_true
  end

  it 'rejects queries with unknown entities' do
    subject.add_entity(entity)

    query = Parser.parse('SELECT Id FROM Bar')
    subject.add_query(query)
    expect(subject.valid?).to be_false
  end

  it 'rejects queries which select unknown fields' do
    subject.add_entity(entity)

    query = Parser.parse('SELECT Bar FROM Foo')
    subject.add_query(query)
    expect(subject.valid?).to be_false
  end

  it 'rejects queries with multiple range predicates' do
    subject.add_entity(entity)
    query = Parser.parse('SELECT Id FROM Foo WHERE Foo.Id > 1 AND Foo.Id < 3')
    subject.add_query(query)
    expect(subject.valid?).to be_false
  end

  it 'rejects queries with unknown fields in where clauses' do
    subject.add_entity(entity)
    query = Parser.parse('SELECT Id FROM Foo WHERE Foo.Bar = 1')
    subject.add_query(query)
    expect(subject.valid?).to be_false
  end

  it 'can find fields on entities from queries' do
    subject.add_entity(entity)
    expect(subject.find_field %w{Foo Id}).to be field
  end

  it 'can find fields which traverse foreign keys' do
    subject.add_entity entity

    other_entity = Entity.new 'Bar'
    other_field = IDField.new 'Quux'
    other_entity << other_field
    subject.add_entity other_entity

    entity << ForeignKey.new('Baz', other_entity)

    expect(subject.find_field %w{Foo Baz Quux}).to be other_field
  end
end
