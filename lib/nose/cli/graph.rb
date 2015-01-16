module NoSE::CLI
  # Add a command to generate a graphic of the schema from a workload
  class NoSECLI < Thor
    desc 'graph WORKLOAD PNG_FILE', 'output a PNG_FILE of the given WORKLOAD'
    option :include_fields, type: :boolean, default: false
    def graph(workload, png_file)
      # rubocop:disable GlobalVars
      require_relative "../../../workloads/#{workload}"
      workload = $workload
      # rubocop:enable GlobalVars

      workload.model.output_png png_file, options[:include_fields]
    end
  end
end
