module NoSE
  describe Schema do
    it 'can model a simple index' do
      schema = NoSE::Schema.new do
        Workload 'rubis'

        # rubocop:disable SingleSpaceBeforeFirstArg
        Index 'users_by_id' do
          Hash  users.id
          Extra users['*']
          Path  users.id
        end
        # rubocop:enable SingleSpaceBeforeFirstArg
      end

      model = schema.workload.model
      users = model.entities['users']

      expect(schema.indexes).to match_array [
        Index.new([users['id']], [], users.fields.values, [users['id']])
      ]
    end

    it 'can model an index over multiple entities' do
      schema = NoSE::Schema.new do
        Workload 'rubis'

        # rubocop:disable SingleSpaceBeforeFirstArg
        Index 'user_region' do
          Hash    users.id
          Ordered regions.id
          Extra   regions.name
          Path    users.id, users.region
        end
        # rubocop:enable SingleSpaceBeforeFirstArg
      end

      model = schema.workload.model
      users = model.entities['users']
      regions = model.entities['regions']

      expect(schema.indexes).to match_array [
        Index.new([users['id']], [regions['id']], [regions['name']],
                  [users['id'], users['region']])
      ]
    end
  end
end
