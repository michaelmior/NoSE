# frozen_string_literal: true

module NoSE
  module CLI
    # Add a command to generate a graphic of the schema from a workload
    class NoSECLI < Thor
      desc 'export', 'export the configuration as environment variables'

      long_desc <<-LONGDESC
        `nose export` reads the configuration file and outputs key-value pairs
        suitable for use as environment variables.
      LONGDESC

      def export
        export_value [], options
      end

      private

      # Recursively export the values
      # @return [void]
      def export_value(path, value)
        if value.is_a? Hash
          value.each do |key, nested_value|
            export_value path + [key], nested_value
          end
        elsif value.is_a? Array
          # Append an integer index to each element of the array
          export_value path + ['count'], value.length
          value.each_with_index do |nested_value, i|
            export_value path + [i], nested_value
          end
        else
          puts "#{path.join('_').upcase}=\"#{value}\""
        end
      end
    end
  end
end
