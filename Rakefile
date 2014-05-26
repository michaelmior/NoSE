require 'rspec/core/rake_task'
require 'yard'

RSpec::Core::RakeTask.new(:spec)
YARD::Rake::YardocTask.new(:doc)

desc 'Run the advisor for a given workload'
task :workload, [:name] do |_, args|
  # rubocop:disable GlobalVars

  $LOAD_PATH.unshift File.dirname(__FILE__) + '/lib'
  require 'sadvisor'

  require_relative "workloads/#{args.name}"

  puts '=========== Indexes ==========='
  indexes = Sadvisor::Search.new($workload).search_overlap
  indexes.each { |index| puts index.inspect }

  # rubocop:enable GlobalVars
end

task default: :spec
