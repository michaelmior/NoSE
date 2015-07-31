module NoSE
  module CLI
    # Add a command to generate a graphic of the schema from a workload
    class NoSECLI < Thor
      desc 'graph WORKLOAD FILE', 'output a FILE of the given WORKLOAD'
      option :include_fields, type: :boolean, default: false, aliases: '-i'
      def graph(workload_name, filename)
        workload = Workload.load workload_name
        type = filename.split('.').last.to_sym
        workload.model.output type, filename, options[:include_fields]
      end
    end
  end
end
