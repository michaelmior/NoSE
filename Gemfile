source 'https://rubygems.org'

gem 'formatador'
gem 'logging'
gem 'parallel'
gem 'parslet', git: 'https://github.com/michaelmior/parslet.git',
               ref: '5d7bb70'
gem 'rake', require: false
gem 'representable'
gem 'ruby-graphviz'
gem 'ruby-mysql'
gem 'smarter_csv'
gem 'thor', require: false

platform :ruby do
  group :gurobi do
    gem 'gurobi', git: 'https://github.com/michaelmior/gurobi.git',
                  ref: '57e0e58'
  end
end

group :test do
  gem 'codeclimate-test-reporter', require: false
  gem 'fakefs', require: 'fakefs/safe'
  gem 'rspec'
  gem 'rspec-collection_matchers'
  gem 'simplecov'
end

platform :ruby do
  group :development do
    gem 'guard-rspec'
    gem 'ruby-prof'
    gem 'pry'
    gem 'pry-byebug'
    gem 'pry-rescue'
    gem 'pry-stack_explorer'
    gem 'yard'
    gem 'yard-thor'
  end
end

group :cassandra do
  gem 'cql-rb'
end
