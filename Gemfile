source 'https://rubygems.org'

gem 'descriptive_statistics', require: false
gem 'faker'
gem 'formatador'
gem 'guruby'
gem 'gruff'
gem 'logging'
gem 'parallel'
gem 'parslet'
gem 'pickup'
gem 'rake', require: false
gem 'representable'
gem 'ruby-graphviz'
gem 'ruby-mysql'  # this is used for the proxy because it's pure Ruby
gem 'smarter_csv'
gem 'table_print'
gem 'thor', require: false

platform :ruby do
  gem 'mysql2'  # this is used for the loader for performance
end

group :test do
  gem 'aruba', require: false
  gem 'codeclimate-test-reporter', require: false
  gem 'fakefs', require: 'fakefs/safe'
  gem 'rspec'
  gem 'rspec-collection_matchers'
  gem 'simplecov', require: false
end

platform :ruby do
  gem 'rbtree'  # for more efficient SortedSet implementation

  group :development do
    gem 'ruby-prof'
    gem 'pry'
    gem 'pry-doc'
    gem 'pry-byebug'
    gem 'pry-rescue'
    gem 'pry-stack_explorer'
    gem 'yard'
    gem 'yard-thor'
  end
end

group :cassandra do
  gem 'cassandra-driver'
end
