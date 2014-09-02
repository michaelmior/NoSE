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

    it 'does nothing if no indexes are enumerated' do
      indexes = Search.new(workload).search_overlap
      expect(indexes).to be_empty
    end

    it 'produces no indices if there is no space' do
      simple_indexes = workload.entities.values.map(&:simple_index)
      simple_size = simple_indexes.map(&:size).inject(0, &:+)
      indexes = Search.new(workload).search_overlap(simple_size + 1)
      expect(indexes).to be_empty
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
