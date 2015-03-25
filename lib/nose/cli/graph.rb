module NoSE::CLI
  # Add a command to generate a graphic of the schema from a workload
  class NoSECLI < Thor
    desc 'graph WORKLOAD PNG_FILE', 'output a PNG_FILE of the given WORKLOAD'
    option :include_fields, type: :boolean, default: false, aliases: '-i'
    def graph(workload, png_file)
      workload = get_workload workload
      workload.model.output_png png_file, options[:include_fields]
    end
  end
end
