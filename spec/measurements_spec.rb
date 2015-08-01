module NoSE::Measurements
  describe StatementMeasurement do
    let(:measurement) do
      m = StatementMeasurement.new 'GROUP', 'NAME', 1.0
      [2, 6, 9, 3, 5, 1, 8, 3, 6, 9, 2].each { |v| m << v }

      m
    end

    it 'can compute averages' do
      expect(measurement.mean).to be_within(0.1).of(4.9)
    end

    it 'can compute percentiles' do
      expect(measurement.percentile(30)).to eq 3.0
    end
  end
end
