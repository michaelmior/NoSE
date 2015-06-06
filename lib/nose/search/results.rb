module NoSE
  module Search
    # A container for results from a schema search
    class Results
      attr_accessor :enumerated_indexes, :indexes, :total_size, :total_cost,
                    :workload, :update_plans, :plans, :cost_model
    end
  end
end
