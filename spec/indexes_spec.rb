require_relative '../lib/indexes'

describe Index do
  before(:each) do
    @entity = Entity.new('Foo')
    @field = IDField.new('Id')
    @entity << @field
  end

  it 'has zero size when empty' do
    expect(Index.new([], []).fields.length).to eq(0)
    expect(Index.new([], []).entry_size).to eq(0)
    expect(Index.new([], []).size).to eq(0)
  end

  it 'contains fields' do
    index = Index.new([@field], [])
    expect(index.has_field? @field).to be_true
  end

  it 'can store additional fields' do
    index = Index.new([], [@field])
    expect(index.has_field? @field).to be_true
  end

  it 'can calculate its size' do
    index = Index.new([@field], [])
    @field *= 10
    expect(index.entry_size).to eq(16)
    expect(index.size).to eq(160)
  end
end
