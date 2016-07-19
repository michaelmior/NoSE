require 'graphviz'
require 'yaml'

module NoSE::CLI
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

    it 'can reformat output', solver: true do
      # Run a simple search and output as JSON
      run_simple 'nose search ebay --format=json'
      json = last_command_stopped.stdout

      # Save the JSON to a file and reformat
      pwd = Dir.pwd
      FakeFS.activate!
      FileUtils.mkdir_p File.expand_path('tmp/aruba', pwd)

      FileUtils.mkdir_p '/tmp'
      File.write '/tmp/x.json', json
      run_simple 'nose reformat --format=yml /tmp/x.json'

      FakeFS.deactivate!

      # Try parsing the reformat output
      expect do
        YAML.load last_command_stopped.stdout
      end.to_not raise_error
    end

    it 'can search with no limits', solver: true do
      run_simple 'nose search rubis --mix=bidding --format=json'
      expect { JSON.parse last_command_stopped.stdout }.to_not raise_error
    end

    it 'can search with a limit', solver: true do
      run_simple 'nose search rubis --mix=bidding ' \
                 '--format=json --max-space=1000000000000'
      expect do
        JSON.parse last_command_stopped.stdout
      end.to_not raise_error
    end

    it 'fails with not enough space', solver: true do
      run 'nose search rubis --max-space=1'
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
  end
end
