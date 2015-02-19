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
        valid_query = Query.new 'SELECT Id FROM Foo WHERE Foo.Id = ?',
                                workload.model
        workload.add_statement valid_query

        expect(workload.queries).to have(1).item
        expect(workload.queries.first).to be_a Query
      end

      it 'only accepts entities and queries' do
        expect { workload << 3 }.to raise_error TypeError
      end
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
      workload.model.output_png '/tmp/rubis.png'
    end

    context 'when generating identity maps' do
      let(:other_entity) do
        Entity.new 'Bar' do
          ID      'Quux'
          Integer 'Corge'
          Integer 'Grault'
        end
      end

      let(:index) do
        Index.new [other_entity['Corge']], [other_entity['Quux']],
                  [other_entity['Grault']], [other_entity]
      end

      it 'produces nothing if there are no updates' do
        workload.model.add_entity other_entity
        expect(workload.identity_maps [index]).to be_empty
      end

      it 'produces nothing if there are no indexes' do
        workload.model.add_entity other_entity
        workload << Update.new('UPDATE Bar SET Corge = ? WHERE Bar.Quux = ?',
                               workload.model)

        expect(workload.identity_maps []).to be_empty
      end

      it 'can generate simple identity maps for updates' do
        workload.model.add_entity other_entity
        workload << Update.new('UPDATE Bar SET Corge = ? WHERE Bar.Quux = ?',
                               workload.model)

        expect(workload.identity_maps [index]).to match_array \
          [other_entity.simple_index]
      end
    end
  end
end
