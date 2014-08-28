module Sadvisor
  describe Parser do
    subject { CQLP.new }

    it 'can parse a simple select' do
      expect(subject.parse 'SELECT foo FROM bar').to be_truthy
    end

    it 'can select many fields' do
      parsed = subject.parse 'SELECT foo, bar, baz FROM quux'
      fields = parsed[:select].map { |atom| atom[:identifier] }
      expect(fields).to eq %w(foo bar baz)
    end

    it 'correctly parses limits' do
      parsed = subject.parse 'SELECT foo FROM baz LIMIT 10'
      expect(parsed[:limit]).to eq '10'
    end

    it 'correctly parses a single field' do
      parsed = subject.parse 'SELECT foo FROM bar'
      expect(parsed[:select].first[:identifier]).to eq 'foo'
    end

    it 'correctly parses a list of fields' do
      parsed = subject.parse 'SELECT foo, bar FROM baz'
      fields = parsed[:select].map { |field| field[:identifier] }
      expect(fields).to eq %w(foo bar)
    end

    it 'correctly parses an order by clause with a single field' do
      parsed = subject.parse 'SELECT foo FROM baz ORDER BY baz.foo'
      field = parsed[:order][:fields].first[:field].map do |atom|
        atom[:identifier]
      end
      expect(field).to eq %w(baz foo)
    end

    it 'correctly parses an order by clause with multiple fields' do
      parsed = subject.parse 'SELECT foo FROM baz ORDER BY baz.foo, baz.bar'
      fields = parsed[:order][:fields].map do |field|
        field[:field].map { |atom| atom[:identifier] }
      end

      expect(fields).to eq [%w(baz foo), %w(baz bar)]
    end

    it 'correctly parses a foreign key traversal' do
      parsed = subject.parse 'SELECT foo FROM baz WHERE baz.bar.quux = ?'
      field = parsed[:where][:expression].first[:field][:field].map do |atom|
        atom[:identifier]
      end
      expect(field).to eq %w(baz bar quux)
    end

    it 'should throw an error on an invalid parse' do
      expect { subject.parse('This is not the CQL you are looking for') }.to \
          raise_error(Exception)
    end

    # XXX Disabled for now since the parser won't do this anymore
    # it 'can find the longest path of entities traversed' do
    #   query =  subject.parse('SELECT foo FROM bar WHERE bar.foo=7 AND ' \
    #                          'bar.foo.baz.quux=3')
    #   expect(query.longest_entity_path).to match_array %w(bar foo baz)
    # end
  end
end
