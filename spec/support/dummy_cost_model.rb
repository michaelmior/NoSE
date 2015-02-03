module NoSE
  RSpec.shared_examples 'dummy_cost_model' do
    let(:cost_model) do
      class DummyCostModel < NoSE::Cost::CostModel
        # Simple cost model which just counts the number of indexes
        def self.index_lookup_cost(_step)
          1
        end
      end

      DummyCostModel
    end
  end
end
