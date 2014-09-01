module Sadvisor
  describe Workload do
    subject(:workload) { Workload.new}
    let(:entity)      { Entity.new('Foo') << field }
    let(:field)       { IDField.new('Id') }

    before(:each) do
      workload.add_entity entity
    end

    context 'when adding items' do
      it 'holds entities' do
        expect(workload.entities).to have(1).item
        expect(workload.entities['Foo']).to be entity
      end

      it 'automatically parses queries' do
        valid_query = Statement.new 'SELECT Id FROM Foo', workload
        workload.add_query(valid_query)

        expect(workload.queries).to have(1).item
        expect(workload.queries.first).to be_a Statement
      end

      it 'only accepts entities and queries' do
        expect { workload << 3 }.to raise_error TypeError
      end
    end

    it 'can find fields on entities from queries' do
      expect(workload.find_field %w(Foo Id)).to be field
    end

    it 'can find fields which traverse foreign keys' do
      other_entity = Entity.new 'Bar'
      other_field = IDField.new 'Quux'
      other_entity << other_field
      workload.add_entity other_entity

      entity << ForeignKey.new('Baz', other_entity)

      expect(workload.find_field %w(Foo Baz Quux)).to be other_field
    end

    it 'raises an exception for nonexistent entities' do
      expect { workload['Bar'] }.to raise_error EntityNotFound
    end
  end
end
