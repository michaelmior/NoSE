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

  puts(("Indexes\n" + '━' * 50).blue)
  indexes = Sadvisor::Search.new($workload).search_overlap
  indexes.each { |index| puts index.inspect }
  puts

  puts(("Query plans\n" + '━' * 50).blue)
  simple_indexes = $workload.entities.values.map(&:simple_index)
  planner = Sadvisor::Planner.new $workload, indexes + simple_indexes
  $workload.queries.each do |query|
    puts query.highlight
    puts planner.min_plan(query).inspect
    puts
  end

  # rubocop:enable GlobalVars
end

task default: :spec
