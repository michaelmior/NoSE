require 'rspec/core/rake_task'
require 'ruby-progressbar'
require 'yard'

RSpec::Core::RakeTask.new(:spec)
YARD::Rake::YardocTask.new(:doc)

desc 'Run the advisor for a given workload'
task :workload, [:name] do |_, args|
  # rubocop:disable GlobalVars

  $LOAD_PATH.unshift File.dirname(__FILE__) + '/lib'
  require 'sadvisor'

  require_relative "workloads/#{args.name}"

  # Display progress while searching
  progress_thread = Thread.new do
    bar = ProgressBar.create title: 'Finding indexes', total: nil
    while true
      bar.increment
      sleep 0.1
    end
  end if $stdout.isatty

  indexes = Sadvisor::Search.new($workload).search_overlap
  simple_indexes = $workload.entities.values.map(&:simple_index)
  if progress_thread
    Thread.kill progress_thread
    puts "\n\n"
  end

  header = "Indexes\n" + '━' * 50
  puts $stdout.isatty ? header.blue : header
  (simple_indexes + indexes).each { |index| puts index.inspect }
  puts

  # Output queries plans for the discovered indices
  header = "Query plans\n" + '━' * 50
  puts $stdout.isatty ? header.blue : header
  planner = Sadvisor::Planner.new $workload, indexes + simple_indexes
  $workload.queries.each do |query|
    puts query.inspect
    puts planner.min_plan(query).inspect
    puts
  end

  # rubocop:enable GlobalVars
end

task default: :spec
