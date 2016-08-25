module NoSE
  describe Schema do
    it 'can model a simple index' do
      schema = Schema.new do
        Model 'rubis'

        Index 'users_by_id' do
          Hash  users.id
          Extra users['*']
          Path  users.id
        end
      end

      model = schema.model
      users = model.entities['users']

      expect(schema.indexes.values).to match_array [
        Index.new([users['id']], [], users.fields.values,
                  QueryGraph::Graph.from_path([users['id']]))
      ]
    end

    it 'can model an index over multiple entities' do
      schema = Schema.new do
        Model 'rubis'

        Index 'user_region' do
          Hash    users.id
          Ordered regions.id
          Extra   regions.name
          Path    users.id, users.region
        end
      end

      model = schema.model
      users = model.entities['users']
      regions = model.entities['regions']

      expect(schema.indexes.values).to match_array [
        Index.new([users['id']], [regions['id']], [regions['name']],
                  QueryGraph::Graph.from_path([users['id'], users['region']]))
      ]
    end
  end
end
