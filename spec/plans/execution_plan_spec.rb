module NoSE
  module Plans
    describe ExecutionPlans do
      it 'can construct an insert plan using the DSL' do
        plans = ExecutionPlans.new do
          Schema 'ebay'

          Plan 'InsertUser' do
            Param users.UserID, :==
            Param users.Name, :==
            Param users.Email, :==
            Insert 'users_by_id'
          end
        end

        plan = plans.groups[nil].first
        index = plans.schema.indexes['users_by_id']
        state = OpenStruct.new cardinality: 1
        expect(plan.update_steps).to eq [
          Plans::InsertPlanStep.new(index, state, index.all_fields)
        ]
      end
    end
  end
end
