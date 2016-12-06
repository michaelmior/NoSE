# frozen_string_literal: true

require 'rspec/core/rake_task'
require 'yard'
require 'yard-thor'
require_relative 'yard_extensions'

# XXX: Patch OpenStruct for yard-thor
class OpenStruct
  def delete(name)
    delete_field name
  end
end

RSpec::Core::RakeTask.new(:spec)
YARD::Rake::YardocTask.new(:doc)

task default: :spec
