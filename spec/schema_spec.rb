module NoSE
  describe Schema do
    it 'can model a simple index' do
      schema = NoSE::Schema.new do
        Workload 'rubis'

        Index 'users_by_id' do
          Hash  users.id
          Extra users['*']
          Path  users.id
        end
      end

      model = schema.workload.model
      users = model.entities['users']

      expect(schema.indexes).to match_array [
        Index.new([users['id']], [], users.fields.values, [users['id']])
      ]
    end
  end
end
