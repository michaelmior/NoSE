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
    other = Entity.new('Bar')
    entity << ToOneKey.new('other', other)

    expect(entity.fields['other'].entity).to be(other)
    expect(entity.fields['other'].type).to eq(:key)
    expect(entity.fields['other'].cardinality).to eq(:one)
  end

  it 'can have foreign keys with cardinality > 1' do
    entity = Entity.new('Foo')
    others = Entity.new('Bar')
    entity << ToManyKey.new('others', others)

    expect(entity.fields['others'].entity).to be(others)
    expect(entity.fields['others'].type).to eq(:key)
    expect(entity.fields['others'].cardinality).to eq(:many)
  end

  it 'can tell fields when they are added' do
    entity = Entity.new('Foo')
    field = IntegerField.new('Bar')

    expect(field.parent).to be_nil

    entity << field

    expect(field.parent).to be(entity)
  end
end
