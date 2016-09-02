# frozen_string_literal: true

module NoSE
  module CLI
    # Add a command to generate a graphic of the schema from a workload
    class NoSECLI < Thor
      desc 'graph WORKLOAD FILE', 'output a FILE of the given WORKLOAD'

      long_desc <<-LONGDESC
        `nose graph` will produce a visual representation of the schema for the
        named workload in the `workloads` directory.
      LONGDESC

      option :include_fields, type: :boolean, default: false, aliases: '-i',
                              desc: 'include each field in the output graph'

      def graph(workload_name, filename)
        workload = Workload.load workload_name
        type = filename.split('.').last.to_sym
        workload.model.output type, filename, options[:include_fields]
      end
    end
  end
end
