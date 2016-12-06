# frozen_string_literal: true

source 'https://rubygems.org'

gem 'faker'
gem 'formatador'
gem 'logging'
gem 'mipper'
gem 'parallel'
gem 'parslet'
gem 'pg'
gem 'pickup'
gem 'pry'
gem 'rake', require: false
gem 'representable'
gem 'ruby-graphviz'
gem 'ruby-mysql' # this is used for the proxy because it's pure Ruby
gem 'sequel'
gem 'smarter_csv'
gem 'thor', require: false

# Required for Cassandra backend
gem 'cassandra-driver'

# Required for MongoDB backend
gem 'mongo'

platform :ruby do
  gem 'rbtree' # for more efficient SortedSet implementation
  gem 'mysql2' # this is used for the loader for performance
end

group :test do
  gem 'fakefs', require: 'fakefs/safe'
  gem 'json-schema'
  gem 'rspec'
  gem 'rspec-collection_matchers'
  gem 'scrutinizer-ocular', require: false
  gem 'simplecov', require: false
end

group :development do
  gem 'pry-rescue'
  gem 'binding_of_caller'
end

platform :ruby do
  group :development do
    gem 'memory_profiler'
    gem 'pry-doc'
    gem 'pry-byebug'
    gem 'pry-stack_explorer'
    gem 'ruby-prof'
    gem 'yard'
    gem 'ronn'
  end
end
