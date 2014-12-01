source 'https://rubygems.org'
source 'https://gem.fury.io/michaelmior/'

gem 'formatador'
gem 'logging'
gem 'parallel'
gem 'parslet', '=1.7.0.mmior.1'
gem 'rake', require: false
gem 'representable'
gem 'ruby-graphviz'
gem 'smarter_csv'
gem 'thor', require: false

platform :ruby do
  group :gurobi do
    gem 'gurobi', '=0.0.2.mmior.1'
  end
end

group :test do
  gem 'rspec'
  gem 'rspec-collection_matchers'
  gem 'simplecov'
end

platform :ruby do
  group :development do
    gem 'ruby-prof'
    gem 'pry'
    gem 'pry-byebug'
    gem 'pry-rescue'
    gem 'pry-stack_explorer'
    gem 'yard'
  end
end

group :cassandra do
  gem 'cql-rb'
end
