#!/usr/bin/env ruby

# Get and print the seed which is used
seed = Random.new_seed
puts "SEED #{seed}"
Random.srand seed

require_relative '../lib/nose'

# Uncomment the line below to enable timing
# NoSE::Timer.enable

factor = ARGV[0].to_i

# Create a random workload generator
network = NoSE::Network.new(nodes_nb: 7 * factor)
workload = NoSE::Workload.new
network.entities.each { |entity| workload << entity }
sgen = NoSE::StatementGenerator.new workload.model

# Add random queries
1.upto(28 * factor).each do
  q = sgen.random_query 1, 3, 1
  workload.add_statement q
end

# Add random updates
1.upto(3 * factor).each do
  u = sgen.random_update 1, 2, 1
  workload.add_statement u
end

# Add random inserts
1.upto(5 * factor).each do
  i = sgen.random_insert 2
  workload.add_statement i
end

# Uncomment the lines below to enable profiling
# (along with the lines above to save the output)
# require 'ruby-prof'
# Parallel.instance_variable_set(:@processor_count, 0)
# RubyProf.start

# Execute NoSE for the random workload and report the time
start = Time.now
indexes = NoSE::IndexEnumerator.new(workload).indexes_for_workload.to_a
search = NoSE::Search::Search.new(workload,
                                  NoSE::Cost::RequestCountCost.new)
search.search_overlap(indexes)
elapsed = Time.now - start
puts "TOTAL: #{elapsed}"

# Uncomment the lines below to save profile output
# (along with the lines above to enable profiling)
# result = RubyProf.stop
# result.eliminate_methods!([
#   /NoSE::Field#hash/,
#   /Range#/,
#   /Array#/,
#   /Set#/,
#   /Hash#/,
#   /Integer#downto/,
#   /Hashids#/,
#   /String#/,
#   /Enumerable#/,
#   /Integer#times/,
#   /Class#new/
# ])
# printer = RubyProf::CallTreePrinter.new(result)
# printer.print(File.open('prof.out', 'w'))
