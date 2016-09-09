module NoSE
  module CLI
    describe NoSECLI do
      include_context 'CLI setup'

      context 'when running the load command' do
        it 'calls the loader with default parameters' do
          expect(loader).to receive(:load).with(
            kind_of(Array),
            kind_of(Hash),
            true,
            nil,
            true
          )
          run_simple 'nose load ebay'
        end

        it 'can load multiple plans' do
          expect(loader).to receive(:load).twice
          run_simple 'nose load ebay rubis_baseline'
        end

        it 'can specify a limit' do
          expect(loader).to receive(:load).with(
            anything, anything, anything,
            10,
            anything
          )
          run_simple 'nose load ebay --limit=10'
        end

        it 'can specify indexes should be loaded even if nonempty' do
          expect(loader).to receive(:load).with(
            anything, anything, anything, anything,
            false
          )
          run_simple 'nose load ebay --no-skip-nonempty'
        end

        it 'can disable progress reporting' do
          expect(loader).to receive(:load).with(
            anything, anything,
            false,
            anything, anything
          )
          run_simple 'nose load ebay --no-progress'
        end
      end
    end
  end
end
