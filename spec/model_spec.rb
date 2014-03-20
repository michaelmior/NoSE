require 'model'

describe Entity do
  subject { Entity.new('Foo') }

  it 'can store fields' do
    subject << IntegerField.new('Bar')
    subject << IntegerField.new('Baz')

    expect(subject.fields.keys).to match_array %w{Bar Baz}
  end

  it 'can have foreign keys' do
    other = Entity.new('Bar') * 100
    field = ToOneKey.new('other', other)
    subject << field

    expect(field.entity).to be(other)
    expect(field.type).to eq(:key)
    expect(field.relationship).to eq(:one)
    expect(field.cardinality).to eq(100)
  end

  it 'can have foreign keys with cardinality > 1' do
    others = Entity.new('Bar') * 100
    field = ToManyKey.new('others', others)
    subject << field

    expect(field.entity).to be(others)
    expect(field.type).to eq(:key)
    expect(field.relationship).to eq(:many)
    expect(field.cardinality).to eq(100)
  end

  it 'can tell fields when they are added' do
    field = IntegerField.new('Bar')

    expect(field.parent).to be_nil

    subject << field

    expect(field.parent).to be(subject)
  end

  it 'can identify a list of key traversals for a field' do
    field = IDField.new('Id')
    subject << field

    expect(subject.key_fields %w{Foo Id}).to eq [field]
  end

  it 'can identify a list of key traversals for foreign keys' do
    field = IDField.new('Id')
    subject << field

    other_entity = Entity.new('Bar')
    other_entity << IntegerField.new('Baz')

    foreign_key = ForeignKey.new('Quux', other_entity)
    subject << foreign_key

    expect(subject.key_fields %w{Foo Quux Baz}).to eq [foreign_key]
  end
end
