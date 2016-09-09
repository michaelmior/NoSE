require 'graphviz'
require 'yaml'

module NoSE
  module CLI
    describe NoSECLI do
      include_context 'CLI setup'

      it 'can output help text' do
        run_simple 'nose help'
        expect(last_command_stopped).to have_output_on_stdout(/^Commands:/)
        expect(last_command_stopped.exit_status).to eq 0
      end

      it 'can output a graph of a workload' do
        expect_any_instance_of(NoSE::Model).to \
          receive(:output).with(:png, '/tmp/rubis.png', false)
        run_simple 'nose graph rubis /tmp/rubis.png'
      end

      context 'when running a search' do
        it 'can search with no limits', solver: true do
          run_simple 'nose search ebay --format=json'
          expect { JSON.parse last_command_stopped.stdout }.to_not raise_error
        end

        it 'can search with a limit', solver: true do
          run_simple 'nose search ebay --format=json --max-space=1000000000000'
          expect do
            JSON.parse last_command_stopped.stdout
          end.to_not raise_error
        end

        it 'fails with not enough space', solver: true do
          run 'nose search ebay --max-space=1'
          expect(last_command_stopped.exit_status).to eq(1)
        end
      end

      it 'can export environment variables' do
        config = { backend: { port: 9042, hosts: ['127.0.0.1'] } }
        pwd = Dir.pwd
        FakeFS.activate!
        FileUtils.mkdir_p File.expand_path('tmp/aruba', pwd)

        FileUtils.mkdir_p File.dirname(NoSECLI::TEST_CONFIG_FILE_NAME)
        File.open(NoSECLI::TEST_CONFIG_FILE_NAME, 'w') do |config_file|
          config_file.write config.to_yaml
        end
        run 'nose export'

        FakeFS.deactivate!

        expect(last_command_stopped).to have_output_on_stdout \
          "BACKEND_PORT=\"9042\"\n" \
          "BACKEND_HOSTS_COUNT=\"1\"\n" \
          "BACKEND_HOSTS_0=\"127.0.0.1\"\n" \
          "PARALLEL=\"false\"\n" \
          "INTERACTIVE=\"true\""
      end

      it 'can produce random plans for a statement in a workload' do
        run_simple 'nose random_plans ebay 1 --format=json'
        json = JSON.parse last_command_stopped.stdout
        expect(json['plans']).to have(1).item
      end

      it 'can create indexes from a schema' do
        expect(backend).to receive(:indexes_ddl).and_return([])
        run_simple 'nose create ebay'
      end

      context 'after producing search output', solver: true do
        before(:each) do
          run_simple 'nose search ebay --format=json'
          json = last_command_stopped.stdout

          pwd = Dir.pwd
          FakeFS.activate!
          FileUtils.mkdir_p File.expand_path('tmp/aruba', pwd)

          FileUtils.mkdir_p '/tmp'
          File.write '/tmp/x.json', json
        end

        it 'can convert to LaTeX' do
          run_simple 'nose texify /tmp/x.json'
          expect(last_command_stopped.stdout).to \
            start_with('\documentclass{article}')
        end

        it 'can reformat output' do
          run_simple 'nose reformat --format=yml /tmp/x.json'

          # Try parsing the reformat output
          expect do
            YAML.load last_command_stopped.stdout
          end.to_not raise_error
        end

        it 'can start a console with predefined plan values' do
          expect(TOPLEVEL_BINDING).to receive(:pry)
          run_simple 'nose console /tmp/x.json'

          expect(TOPLEVEL_BINDING.local_variable_get(:workload)).to \
            be_a Workload
          expect(TOPLEVEL_BINDING.local_variable_get(:model)).to \
            be_a Model
          expect(TOPLEVEL_BINDING.local_variable_get(:backend)).to \
            eq(backend)
          expect(TOPLEVEL_BINDING.local_variable_get(:indexes).first).to \
            be_a Index
        end

        it 'can recalculate costs' do
          expect(cost_model).to receive(:index_lookup_cost) \
            .at_least(:once).and_return(10)

          run_simple 'nose recost /tmp/x.json dummy'
          json = JSON.parse last_command_stopped.stdout
          expect(json['plans'].last['cost']).to eq(10)
        end

        after(:each) { FakeFS.deactivate! }
      end
    end
  end
end
