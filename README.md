# NoSQL Schema Evaluator (NoSE)

[![Build Status](https://magnum.travis-ci.com/michaelmior/sadvisor.svg?token=rM4RuzPrnmeRRxXcrK4C&branch=master)](https://magnum.travis-ci.com/michaelmior/sadvisor)

This is a work in progress tool to provide automated physical schema design for NoSQL data stores.

Testing has been done with Ruby 2+ with dependencies managed via [bundler](http://bundler.io/).
Most of the code should also run under the latest [JRuby](http://jruby.org/).
To get started, run `bundle install` to install the necessary dependencies.
However, under JRuby, any code depending on C extensions or MRI internals should be excluded with `--without=development gurobi mysql`.
The `GUROBI_HOME` environment must be set to a valid [Gurobi](http://www.gurobi.com/) installation.
Note that this project depends on forks of several gems.

Examples of the workload input format is given in the `workloads/` directory.
To run the schema advisor, simply execute the command below

    bundle exec nose workload rubis

All source code is documented and more details on the command line tool can be retrieved with `bundle exec nose help`.
You can view complete documentation by running `bundle exec rake doc` and viewing the output in the `doc/` directory.
Tests are written using [RSpec](http://rspec.info/) and can be executed with `bundle exec rspec`.
If you do not have a copy of Gurobi available, you can exclude these tests with `--tag ~gurobi`.

Some commands require a configuration file in lieu of command line options.
An example configuration file for the different components of NoSE is given in [nose.yml.example](nose.yml.example).

## Publications

![ACM DL Author-ize service](http://dl.acm.org/images/oa.gif) Michael J. Mior. 2014. [Automated schema design for NoSQL databases](http://dl.acm.org/authorize?N71145). In Proceedings of the 2014 SIGMOD PhD symposium (SIGMOD'14 PhD Symposium). ACM, New York, NY, USA, 41-45.
