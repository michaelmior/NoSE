require 'sadvisor'

describe Parser do
  it 'can parse a simple select' do
    expect(Parser.parse('SELECT foo FROM bar')).to be_true
  end

  it 'correctly parses limits' do
    expect(Parser.parse('SELECT foo FROM baz LIMIT 10').limit).to eq(10)
  end

  it 'correctly parses integer literals' do
    where = Parser.parse('SELECT foo FROM bar WHERE baz.foo = 3').where
    expect(where[0].value).to eq(3)
  end

  it 'correctly parses float literals' do
    where = Parser.parse('SELECT foo FROM bar WHERE baz.bar = 2.7').where
    expect(where[0].value).to eq(2.7)
  end

  it 'correctly parses string literals' do
    where = Parser.parse('SELECT foo FROM bar WHERE baz.bar = "quux"').where
    expect(where[0].value).to eq('quux')
  end

  it 'correctly parses a single field' do
    expect(Parser.parse('SELECT foo FROM bar').fields[0].value).to \
        eq %w{bar foo}
  end

  it 'correctly parses a list of fields' do
    expect(Parser.parse('SELECT foo, bar FROM baz').fields.map do |field|
      field.value
    end).to match_array [%w{baz foo}, %w{baz bar}]
  end

  it 'correctly parses an order by clause with a single field' do
    expect(Parser.parse('SELECT foo FROM baz ORDER BY baz.foo').order_by).to \
        match_array([%w{baz foo}])
  end

  it 'correctly parses an order by clause with multiple fields' do
    expect(Parser.parse('SELECT foo FROM baz ' \
                        'ORDER BY baz.foo, baz.bar').order_by).to \
                        eq([%w{baz foo}, %w{baz bar}])
  end

  it 'correctly parses a foreign key traversal' do
    query = Parser.parse('SELECT foo FROM baz WHERE baz.bar.quux = 3')
    expect(query.eq_fields[0].field.value).to eq(%w{baz bar quux})
  end

  it 'should throw an error on an invalid parse' do
    expect { Parser.parse('This is not the CQL you are looking for') }.to \
        raise_error(Exception)
  end
end
