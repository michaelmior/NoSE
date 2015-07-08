module NoSE
  RSpec.shared_examples 'dummy_cost_model' do
    let(:cost_model) do
      class DummyCost < NoSE::Cost::Cost
        include Subtype

        # Simple cost model which just counts the number of indexes
        def index_lookup_cost(_step)
          1
        end
      end

      DummyCost.new
    end
  end
end
