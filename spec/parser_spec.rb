require_relative '../parser/parser'

describe Parser do
  it 'can parse a simple select' do
    expect(Parser.parse('SELECT foo.bar FROM baz')).to be_true
  end

  it 'correctly parses limits' do
    expect(Parser.parse('SELECT foo.bar FROM baz LIMIT 10').limit).to be(10)
  end
end
