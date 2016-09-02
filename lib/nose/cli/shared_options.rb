# frozen_string_literal: true

module NoSE
  module CLI
    # Add the use of shared options
    class NoSECLI < Thor
      # Add a new option to those which can be potentially shared
      def self.share_option(name, options = {})
        @options ||= {}
        @options[name] = options
      end

      # Use a shared option for the current command
      # @return [void]
      def self.shared_option(name)
        method_option name, @options[name]
      end

      share_option :mix, type: :string, default: 'default',
                         desc: 'the name of the mix for weighting queries'
      share_option :format, type: :string, default: 'txt',
                            enum: %w(txt json yml html), aliases: '-f',
                            desc: 'the format of the produced plans'
      share_option :output, type: :string, default: nil, aliases: '-o',
                            banner: 'FILE',
                            desc: 'a file where produced plans ' \
                                  'should be stored'
    end
  end
end
