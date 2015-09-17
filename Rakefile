require 'rspec/core/rake_task'
require 'yard'
require 'yard-thor'

# XXX: Patch OpenStruct for yard-thor
class OpenStruct
  def delete(name)
    delete_field name
  end
end

RSpec::Core::RakeTask.new(:spec)
YARD::Rake::YardocTask.new(:doc)

task :console do
  require 'irb'
  require 'irb/completion'
  require_relative './lib/nose'
  ARGV.clear
  IRB.start
end

task :man do
  require 'erb'
  require 'fakefs/safe'
  require 'fileutils'
  require 'ronn'
  require_relative './lib/nose/cli'

  # Create the Markdown using ERB
  ns = OpenStruct.new commands: NoSE::CLI::NoSECLI.commands,
                      options: NoSE::CLI::NoSECLI.class_options
  tmpl = File.read File.join(File.dirname(__FILE__), 'man.erb')
  out = ERB.new(tmpl, nil, '>').result(ns.instance_eval { binding })

  # Write the generated Markdown to a fake file then process with ronn
  FakeFS.activate!

  path = 'nose.md'
  File.open(path, 'w') { |f| f.write out }
  doc = Ronn::Document.new(path)
  roff = doc.convert('roff')

  FakeFS.deactivate!

  # Output the generated man page
  FileUtils.mkdir_p 'man'
  File.open('man/nose.1', 'w') { |f| f.write roff }
end

task default: :spec
