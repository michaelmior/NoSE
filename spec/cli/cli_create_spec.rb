module NoSE
  module CLI
    describe NoSECLI do
      include_context 'CLI setup'

      context 'when running the create command' do
        it 'outputs the produced DDL' do
          expect(backend).to receive(:indexes_ddl).and_return(%w(foo bar))
          run_simple 'nose create ebay'
          expect(last_command_stopped.output).to eq "foo\nbar\n"
        end

        it 'does not try to create on a dry run' do
          expect(backend).to receive(:indexes_ddl).with(
            false,
            anything, anything
          ).and_return([])
          run_simple 'nose create ebay --dry-run'
        end

        it 'can specify that existing indexes should be skipped' do
          expect(backend).to receive(:indexes_ddl).with(
            anything,
            true,
            anything
          ).and_return([])
          run_simple 'nose create ebay --skip-existing'
        end

        it 'can specify that existing indexes should be dropped' do
          expect(backend).to receive(:indexes_ddl).with(
            anything, anything,
            true
          ).and_return([])
          run_simple 'nose create ebay --drop-existing'
        end
      end
    end
  end
end
