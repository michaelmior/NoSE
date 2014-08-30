# Schema Advisor

This is a work in progress tool to provide automated physical schema design for NoSQL data stores.

This project is written in Ruby and uses [GLPK](https://www.gnu.org/software/glpk/) as a solver for linear programming problems.
Testing has been done with Ruby 2+ with dependencies managed via [bundler](http://bundler.io/).
To get started, run `bundle install` to install the necessary dependencies.
The `GUROBI_HOME` environment must be set to a valid [Gurobi](http://www.gurobi.com/) installation.
Note that this project depends on forks of several gems.

An example of the workload input format is given in the `workloads/` directory.
To run the schema advisor, simply execute the command below

    bundle exec rake 'workload[rubis]'

All source code is documented.
You can view complete documentation by running `bundle exec rake doc` and viewing the output in the `doc/` directory.
Tests are written using [RSpec](http://rspec.info/) and can be executed with `bundle exec rake spec`.

## Publications

Michael J. Mior. 2014. [Automated schema design for NoSQL databases](http://doi.acm.org/10.1145/2602622.2602624). In Proceedings of the 2014 SIGMOD PhD symposium (SIGMOD'14 PhD Symposium). ACM, New York, NY, USA, 41-45.

