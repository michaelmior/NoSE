require_relative '../lib/model'

describe Entity do
  it 'can store fields' do
    entity = Entity.new('Foo')
    entity << IntegerField.new('Bar')
    entity << IntegerField.new('Baz')

    expect(entity.fields.keys).to match_array(['Bar', 'Baz'])
  end

  it 'can have foreign keys' do
    entity = Entity.new('Foo')
    other = Entity.new('Bar') * 100
    field = ToOneKey.new('other', other)
    entity << field

    expect(field.entity).to be(other)
    expect(field.type).to eq(:key)
    expect(field.relationship).to eq(:one)
    expect(field.cardinality).to eq(100)
  end

  it 'can have foreign keys with cardinality > 1' do
    entity = Entity.new('Foo')
    others = Entity.new('Bar') * 100
    field = ToManyKey.new('others', others)
    entity << field

    expect(field.entity).to be(others)
    expect(field.type).to eq(:key)
    expect(field.relationship).to eq(:many)
    expect(field.cardinality).to eq(100)
  end

  it 'can tell fields when they are added' do
    entity = Entity.new('Foo')
    field = IntegerField.new('Bar')

    expect(field.parent).to be_nil

    entity << field

    expect(field.parent).to be(entity)
  end
end
