module NoSE
  describe Workload do
    subject(:workload) { Workload.new }
    let(:entity)      { Entity.new('Foo') << field }
    let(:field)       { Fields::IDField.new('Id') }

    before(:each) do
      workload.model.add_entity entity
    end

    context 'when adding items' do
      it 'holds entities' do
        expect(workload.model.entities).to have(1).item
        expect(workload.model['Foo']).to be entity
      end

      it 'automatically parses queries' do
        valid_query = Statement.parse 'SELECT Foo.Id FROM Foo ' \
                                      'WHERE Foo.Id = ?', workload.model
        workload.add_statement valid_query

        expect(workload.queries).to have(1).item
        expect(workload.queries.first).to be_a Query
      end

      it 'only accepts entities and queries' do
        expect { workload << 3 }.to raise_error TypeError
      end
    end

    it 'can find statements with a given tag' do
      query = Statement.parse 'SELECT Foo.Id FROM Foo WHERE Foo.Id = ? -- foo',
                              workload.model
      workload.add_statement query

      expect(workload.find_with_tag 'foo').to eq(query)
    end

    it 'can find fields on entities from queries' do
      expect(workload.model.find_field %w(Foo Id)).to be field
    end

    it 'can find fields which traverse foreign keys' do
      other_entity = Entity.new 'Bar'
      other_field = Fields::IDField.new 'Quux'
      other_entity << other_field
      workload.model.add_entity other_entity

      entity << Fields::ForeignKeyField.new('Baz', other_entity)

      expect(workload.model.find_field %w(Foo Baz Quux)).to be other_field
    end

    it 'raises an exception for nonexistent entities' do
      expect { workload.model['Bar'] }.to raise_error EntityNotFound
    end

    it 'can produce an image of itself' do
      expect_any_instance_of(GraphViz).to \
        receive(:output).with(png: '/tmp/rubis.png')
      workload.model.output :png, '/tmp/rubis.png'
    end

    it 'can remove updates' do
      entity << Fields::IntegerField.new('Bar')

      valid_query = Statement.parse 'SELECT Foo.Id FROM Foo WHERE Foo.Id = ?',
                                    workload.model
      workload.add_statement valid_query
      update = Statement.parse 'UPDATE Foo SET Bar = ? WHERE Foo.Id = ?',
                               workload.model
      workload.add_statement update

      workload.remove_updates
      expect(workload.queries).not_to be_empty
      expect(workload.updates).to be_empty
    end

    it 'can group statements' do
      query1 = 'SELECT Foo.Bar FROM Foo WHERE Foo.Id = ?'
      query2 = 'SELECT Foo.Baz FROM Foo WHERE Foo.Id = ?'

      workload = Workload.new do
        Entity 'Foo' do
          ID 'Id'
          String 'Bar'
          String 'Baz'
        end

        Group 'Test1', 0.5 do
          Q query1
        end

        Group 'Test2', 0.5 do
          Q query2
        end
      end

      expect(workload.statement_weights).to eq(
        Statement.parse(query1, workload.model) => 0.5,
        Statement.parse(query2, workload.model) => 0.5
      )
    end
  end
end
