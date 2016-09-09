module NoSE
  module CLI
    RSpec.shared_context 'CLI setup' do
      include_context 'dummy cost model'

      let(:backend) { instance_double(Backend::BackendBase) }
      let(:loader) { instance_double(Loader::LoaderBase) }

      before(:each) do
        allow(CLI::NoSECLI).to receive(:new) \
          .and_wrap_original do |method, *args, &block|
          cli = method.call(*args, &block)

          cli.instance_variable_set :@backend, backend

          loader_class = class_double(Loader::LoaderBase)
          allow(loader_class).to receive(:new).and_return(loader)
          cli.instance_variable_set :@loader_class, loader_class

          cost_class = class_double(Cost::Cost)
          allow(cost_class).to receive(:new).and_return(cost_model)
          cli.instance_variable_set :@cost_class, cost_class

          cli
        end
      end
    end
  end
end
