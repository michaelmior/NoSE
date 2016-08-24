module NoSE
  module Search
    describe Results do
      include_context 'entities'

      let(:problem) do
        OpenStruct.new(
          objective_type: Objective::COST,
          query_vars: []
        )
      end

      let(:results) do
        r = Results.new problem
        r.workload = workload
        r.enumerated_indexes = []
        r.indexes = []
        r.plans = []
        r.update_plans = []
        r.indexes = []
        r.total_cost = 0
        r.total_size = 0

        r
      end

      it 'can ensure only enumerated indexes are used' do
        index = Index.new [tweet['TweetId']], [], [tweet['Body']],
                          QueryGraph::Graph.from_path([tweet.id_field])

        results.indexes = [index]
        expect { results.validate }.to \
          raise_error InvalidResultsException
      end

      it 'checks for the correct cost objective value' do
        results.total_cost = 1
        expect { results.validate }.to raise_error InvalidResultsException
      end

      it 'checks for the correct size objective value' do
        problem.objective_type = Objective::SPACE
        results.total_size = 1
        expect { results.validate }.to raise_error InvalidResultsException
      end
    end
  end
end
