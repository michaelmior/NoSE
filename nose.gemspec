Gem::Specification.new do |s|
  s.name        = 'nose'
  s.version     = '0.1.4'
  s.license     = 'GPL-3.0'
  s.summary     = 'Schema design for NoSQL applications'
  s.author      = 'Michael Mior'
  s.email       = 'mmior@uwaterloo.ca'
  s.files       = Dir['lib/**/*'] +
                  Dir['templates/*'] +
                  Dir['models/*'] +
                  Dir['workloads/*'] +
                  Dir['plans/*'] +
                  Dir['schemas/*'] +
                  Dir['data/**/*']
  s.homepage    = 'https://michael.mior.ca/projects/NoSE/'

  s.add_dependency 'faker', '~> 2.14.0', '>= 1.7.0'
  s.add_dependency 'formatador', '~> 0.2.5'
  s.add_dependency 'json-schema', '~> 2.8.0', '>= 2.8.0'
  s.add_dependency 'logging', '~> 2.2.0', '>= 2.2.0'
  s.add_dependency 'mipper', '~> 0.1.0'
  s.add_dependency 'parallel', '~> 1.17.0', '>= 1.11.0'
  s.add_dependency 'parslet', '~> 1.8.0', '>= 1.8.0'
  s.add_dependency 'pg', '~> 0.20.0'
  s.add_dependency 'pickup', '~> 0.0.11'
  s.add_dependency 'pry', '~> 0.13.1'
  s.add_dependency 'rake', '~> 12.3.2', '>= 12.0.0'
  s.add_dependency 'representable', '~> 3.0.0', '>= 3.0.0'
  s.add_dependency 'ruby-graphviz', '~> 1.2.2', '>= 1.2.0'
  s.add_dependency 'ruby-mysql', '~> 2.9.14', '>= 2.9.0' # for the proxy because it's pure Ruby
  s.add_dependency 'sequel', '~> 5.35.0', '>= 4.46.0'
  s.add_dependency 'smarter_csv', '~> 1.2.6', '>= 1.1.0'

  # Required for Cassandra backend
  s.add_dependency 'cassandra-driver', '~> 3.2.3', '>= 3.1.0'

  # Required for MongoDB backend
  s.add_dependency 'mongo', '~> 2.13.0', '>= 2.4.0'

  s.add_development_dependency 'fakefs', '~> 1.2.2'
  s.add_development_dependency 'memory_profiler', '~> 0.9.7'
  s.add_development_dependency 'pry-byebug', '~> 3.9.0'
  s.add_development_dependency 'pry-doc', '~> 1.1.0'
  s.add_development_dependency 'pry-stack_explorer', '~> 0.4.11'
  s.add_development_dependency 'ronn', '~> 0.7.3'
  s.add_development_dependency 'rspec', '~> 3.9.0'
  s.add_development_dependency 'rspec-core', '~> 3.9.0'
  s.add_development_dependency 'rspec-collection_matchers', '~> 1.2.0', '>= 1.1.0'
  s.add_development_dependency 'ruby-prof', '~> 0.18.0'
  s.add_development_dependency 'scrutinizer-ocular', '~> 1.0.1', '>= 1.0.0'
  s.add_development_dependency 'simplecov', '~> 0.19.0'
  s.add_development_dependency 'yard', '~> 0.9.4'

  # Below for MRI only (TODO JRuby gemspec)
  s.add_dependency 'rbtree', '~> 0.4.2' # for more efficient SortedSet implementation
  s.add_dependency 'mysql2', '~> 0.5.2' # this is used for the loader for performance
  s.add_development_dependency 'pry-rescue', '~> 1.5.1'
  s.add_development_dependency 'binding_of_caller', '~> 0.8.0'
end
