# frozen_string_literal: true

require 'erb'
require 'shellwords'

module NoSE
  module CLI
    # Add a command to generate a shell completion script for nose
    class NoSECLI < Thor
      desc 'completions', 'generate a shell completion script for nose'

      long_desc <<-LONGDESC
        `nose completions` generates a shell script which can be sourced to
        autocomplete the commands and options provided by nose.
      LONGDESC

      def completions
        commands = NoSE::CLI::NoSECLI.all_commands.to_a.sort_by(&:first)

        tmpl = File.read File.join(File.dirname(__FILE__),
                                   '../../../templates/completions.erb')
        ns = OpenStruct.new commands: commands
        puts ERB.new(tmpl, nil, '>').result(ns.instance_eval { binding })
      end
    end
  end
end
