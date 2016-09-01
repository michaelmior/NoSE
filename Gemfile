source 'https://rubygems.org'

gem 'ansi-to-html'
gem 'descriptive_statistics', require: false
gem 'faker'
gem 'formatador'
gem 'gruff'
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
gem 'table_print'
gem 'thor', require: false

platform :ruby do
  gem 'mysql2' # this is used for the loader for performance
end

group :test do
  gem 'aruba', require: false
  gem 'scrutinizer-ocular', require: false
  gem 'fakefs', require: 'fakefs/safe'
  gem 'rspec'
  gem 'rspec-collection_matchers'
  gem 'simplecov', require: false
end

group :development do
  gem 'pry-rescue'
  gem 'binding_of_caller'
end

platform :ruby do
  gem 'rbtree' # for more efficient SortedSet implementation

  group :development do
    gem 'ruby-prof'
    gem 'pry-doc'
    gem 'pry-byebug'
    gem 'pry-stack_explorer'
    gem 'yard'
    gem 'yard-thor'
    gem 'ronn'
  end
end

group :cassandra do
  gem 'cassandra-driver'
end
