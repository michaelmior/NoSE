# frozen_string_literal: true

module NoSE
  module CLI
    # Add a command to generate a workload file from a given loader
    class NoSECLI < Thor
      desc 'genworkload NAME',
           'generate a workload called NAME from the configured loader'

      long_desc <<-LONGDESC
        `nose genworkload` will produce a new file in the `workloads` directory
        containing information on the workload from the configured loader.
      LONGDESC

      def genworkload(name)
        loader_class = get_class 'loader', options
        workload = loader_class.new.workload options[:loader]
        File.open("./workloads/#{name}.rb", 'w') do |file|
          file.write workload.source_code
        end
      end
    end
  end
end
