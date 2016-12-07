Gem::Specification.new do |s|
  s.name        = 'nose'
  s.version     = '0.1.0pre'
  s.license     = 'GPL-3.0'
  s.summary     = 'Schema design for NoSQL applications'
  s.author      = 'Michael Mior'
  s.email       = 'mmior@uwaterloo.ca'
  s.files       = Dir['lib/**/*'] + Dir['templates/*']
  s.homepage    = 'https://michael.mior.ca/projects/NoSE/'

  s.add_dependency 'faker'
  s.add_dependency 'formatador'
  s.add_dependency 'logging'
  s.add_dependency 'mipper'
  s.add_dependency 'parallel'
  s.add_dependency 'parslet'
  s.add_dependency 'pg'
  s.add_dependency 'pickup'
  s.add_dependency 'pry'
  s.add_dependency 'rake'
  s.add_dependency 'representable', '3.0.0'
  s.add_dependency 'ruby-graphviz'
  s.add_dependency 'ruby-mysql' # for the proxy because it's pure Ruby
  s.add_dependency 'sequel'
  s.add_dependency 'smarter_csv'

  # Required for Cassandra backend
  s.add_dependency 'cassandra-driver'

  # Required for MongoDB backend
  s.add_dependency 'mongo'

  s.add_development_dependency 'fakefs'
  s.add_development_dependency 'json-schema'
  s.add_development_dependency 'memory_profiler'
  s.add_development_dependency 'pry-byebug'
  s.add_development_dependency 'pry-doc'
  s.add_development_dependency 'pry-stack_explorer'
  s.add_development_dependency 'ronn'
  s.add_development_dependency 'rspec'
  s.add_development_dependency 'rspec-core'
  s.add_development_dependency 'rspec-collection_matchers'
  s.add_development_dependency 'ruby-prof'
  s.add_development_dependency 'scrutinizer-ocular'
  s.add_development_dependency 'simplecov'
  s.add_development_dependency 'yard'

  # Below for MRI only (TODO JRuby gemspec)
  s.add_dependency 'rbtree' # for more efficient SortedSet implementation
  s.add_dependency 'mysql2' # this is used for the loader for performance
  s.add_development_dependency 'pry-rescue'
  s.add_development_dependency 'binding_of_caller'
end
