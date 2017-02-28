module NoSE
  module Serialize
    describe EntityFieldRepresenter do
      include_context 'entities'

      it 'serializes a field and its properties' do
        field = EntityFieldRepresenter.represent(user['Username']).to_hash
        expect(field).to eq(
          'name' => 'Username',
          'size' => 10,
          'cardinality' => 10,
          'type' => 'string'
        )
      end
    end

    describe FieldRepresenter do
      include_context 'entities'

      it 'serializes a field to a simple hash' do
        field = FieldRepresenter.represent(user['Username']).to_hash
        expect(field).to eq('name' => 'Username', 'parent' => 'User')
      end
    end

    describe IndexRepresenter do
      include_context 'entities'

      it 'serializes an index to a key' do
        index = Index.new [user['Username']], [user['UserId']], [],
                          QueryGraph::Graph.from_path([user.id_field]),
                          saved_key: 'IndexKey'
        hash = IndexRepresenter.represent(index).to_hash
        expect(hash).to eq('key' => 'IndexKey')
      end
    end

    describe EntityRepresenter do
      it 'serializes an empty entity' do
        entity = Entity.new('Foo') * 10
        hash = EntityRepresenter.represent(entity).to_hash
        expect(hash).to eq('name' => 'Foo', 'count' => 10, 'fields' => [])
      end
    end

    describe StatementRepresenter do
      include_context 'entities'

      it 'serializes queries to a string' do
        expect(StatementRepresenter.represent(query).to_hash).to eq(query.text)
      end
    end
  end
end
