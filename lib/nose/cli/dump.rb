module NoSE
    module CLI
    # Add a command to dump a workload and its corresponding schema
    class NoSECLI < Thor
      desc 'dump SCHEMA', 'output the schema in  SCHEMA'
      option :format, type: :string, default: 'txt',
                      enum: %w(txt json yml), aliases: '-f'
      def dump(schema_name)
        schema = Schema.load schema_name

        results = OpenStruct.new
        results.workload = schema.workload
        results.indexes = schema.indexes.values
        results.enumerated_indexes = []
        results.plans = []
        results.update_plans = []

        cost_model = get_class('cost', options[:cost_model][:name])
        results.cost_model = cost_model.new(**options[:cost_model])

        # Output the results in the specified format
        send(('output_' + options[:format]).to_sym, results)
      end
    end
  end
end

