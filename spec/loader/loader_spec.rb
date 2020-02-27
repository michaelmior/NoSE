require 'nose/loader'

module NoSE
  module Loader
    describe LoaderBase do
      it 'fails on methoods intended to be overridden' do
        loader = LoaderBase.new
        expect { loader.workload(nil) }.to raise_error NotImplementedError
        expect { loader.load(nil, nil) }.to raise_error NotImplementedError
        expect { loader.model(nil) }.to raise_error NotImplementedError
      end
    end
  end
end
