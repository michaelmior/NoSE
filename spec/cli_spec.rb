require 'graphviz'

module NoSE::CLI
  describe NoSECLI do
    it 'can output help text' do
      run_simple 'nose help'
      expect(all_output).to start_with 'Commands:'
      expect(last_exit_status).to eq 0
    end

    it 'can output a graph of a workload' do
      expect_any_instance_of(NoSE::Model).to \
        receive(:output).with(:png, '/tmp/rubis.png', false)
      run_simple 'nose graph rubis /tmp/rubis.png'
    end

    it 'can reformat output' do
      # Run a simple search and output as JSON
      run_simple 'nose search rubis-synthetic --format=json'
      json = all_output

      # Save the JSON to a file and reformat
      FakeFS.activate!

      FileUtils.mkdir_p '/tmp'
      File.write '/tmp/x.json', json
      reformat_cmd = 'nose reformat --format=yml /tmp/x.json'
      run_simple reformat_cmd

      FakeFS.deactivate!

      # Try parsing the reformat output
      expect { YAML.load get_process(reformat_cmd).stdout }.to_not raise_error
    end

    it 'can search with no limits', gurobi: true do
      search_cmd = 'nose search rubis --format=json'
      run_simple search_cmd
      expect { JSON.parse get_process(search_cmd).stdout }.to_not raise_error
    end

    it 'can search with a limit', gurobi: true do
      search_cmd = 'nose search rubis --format=json --max-space=1000000000000'
      run_simple search_cmd
      expect { JSON.parse get_process(search_cmd).stdout }.to_not raise_error
    end

    it 'fails with not enough space', gurobi: true do
      run 'nose search rubis --max-space=1'
      expect(last_exit_status).to eq(1)
    end
  end
end
