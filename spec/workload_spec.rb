module Sadvisor
  describe Workload do
    let(:entity)      { Entity.new('Foo') << field }
    let(:field)       { IDField.new('Id') }

    before(:each) do
      subject.add_entity entity
    end

    it 'holds queries and entities' do
      expect(subject.entities).to have(1).item
      expect(subject.entities['Foo']).to be entity

      valid_query = Statement.new 'SELECT Id FROM Foo', subject

      subject.add_query(valid_query)
      expect(subject.queries).to match_array [valid_query]
    end

    it 'can find fields on entities from queries' do
      expect(subject.find_field %w(Foo Id)).to be field
    end

    it 'can find fields which traverse foreign keys' do
      other_entity = Entity.new 'Bar'
      other_field = IDField.new 'Quux'
      other_entity << other_field
      subject.add_entity other_entity

      entity << ForeignKey.new('Baz', other_entity)

      expect(subject.find_field %w(Foo Baz Quux)).to be other_field
    end
  end
end
