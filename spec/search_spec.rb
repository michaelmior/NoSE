module Sadvisor
  describe Search do
    let(:workload) do
      Workload.new do
        Entity 'foo' do
          ID 'id'
          String 'bar'
          Integer 'baz'
        end
      end
    end

    it 'raises an exception if there is no space' do
      workload.add_query 'SELECT id FROM foo'
      expect { Search.new(workload).search_overlap(1) }.to raise_error
    end

    it 'produces a materialized view with sufficient space' do
      query = Statement.new 'SELECT id FROM foo WHERE foo.bar = ? ' \
                            'ORDER BY foo.baz', workload
      workload.add_query query
      indexes = Search.new(workload).search_overlap
      expect(indexes).to include query.materialize_view
    end
  end
end
