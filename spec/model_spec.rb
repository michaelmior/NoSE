describe Sadvisor::Entity do
  subject { Sadvisor::Entity.new('Foo') }

  it 'can store fields' do
    subject << Sadvisor::IntegerField.new('Bar')
    subject << Sadvisor::IntegerField.new('Baz')

    expect(subject.fields.keys).to match_array %w{Bar Baz}
  end

  it 'can have foreign keys' do
    other = subject * 100
    field = Sadvisor::ToOneKey.new('other', other)
    subject << field

    expect(field.entity).to be(other)
    expect(field.type).to eq(:key)
    expect(field.relationship).to eq(:one)
    expect(field.cardinality).to eq(100)
  end

  it 'can have foreign keys with cardinality > 1' do
    others = subject * 100
    field = Sadvisor::ToManyKey.new('others', others)
    subject << field

    expect(field.entity).to be(others)
    expect(field.type).to eq(:key)
    expect(field.relationship).to eq(:many)
    expect(field.cardinality).to eq(100)
  end

  it 'can tell fields when they are added' do
    field = Sadvisor::IntegerField.new('Bar')

    expect(field.parent).to be_nil

    subject << field

    expect(field.parent).to be(subject)
  end

  it 'can identify a list of key traversals for a field' do
    field = Sadvisor::IDField.new('Id')
    subject << field

    expect(subject.key_fields %w{Foo Id}).to eq [field]
  end

  it 'can identify a list of key traversals for foreign keys' do
    field = Sadvisor::IDField.new('Id')
    subject << field

    other_entity = Sadvisor::Entity.new('Bar')
    other_entity << Sadvisor::IntegerField.new('Baz')

    foreign_key = Sadvisor::ForeignKey.new('Quux', other_entity)
    subject << foreign_key

    expect(subject.key_fields %w{Foo Quux Baz}).to eq [foreign_key]
  end

  it 'can create entities using a DSL' do
    entity = Sadvisor::Entity.new 'Foo' do
      ID      'Bar'
      Integer 'Baz'
      String  'Quux', 20
    end

    expect(entity.fields.count).to eq 3
    expect(entity.fields['Quux'].size).to eq 20
  end
end
