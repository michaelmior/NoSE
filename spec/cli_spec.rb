require 'graphviz'
require 'yaml'

module NoSE
  module CLI
    describe NoSECLI do
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

        it 'can convert to latex' do
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
          FileUtils.mkdir_p File.dirname(NoSECLI::TEST_CONFIG_FILE_NAME)
          File.open(NoSECLI::TEST_CONFIG_FILE_NAME, 'w') do |config_file|
            config_file.write({ backend: { name: 'file' } }.to_yaml)
          end

          expect(TOPLEVEL_BINDING).to receive(:pry)
          run_simple 'nose console /tmp/x.json'

          expect(TOPLEVEL_BINDING.local_variable_get(:workload)).to \
            be_a Workload
          expect(TOPLEVEL_BINDING.local_variable_get(:model)).to \
            be_a Model
          expect(TOPLEVEL_BINDING.local_variable_get(:backend)).to \
            be_a Backend::FileBackend
          expect(TOPLEVEL_BINDING.local_variable_get(:indexes).first).to \
            be_a Index
        end

        after(:each) { FakeFS.deactivate! }
      end
    end
  end
end
