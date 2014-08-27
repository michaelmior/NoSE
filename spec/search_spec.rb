module Sadvisor
  describe Search do
    it 'does nothing if no indexes are enumerated' do
      workload = Workload.new
      indexes = Search.new(workload).search_overlap
      expect(indexes).to be_empty
    end
  end
end
