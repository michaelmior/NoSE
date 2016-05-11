# NoSQL Schema Evaluator (NoSE)

[![Build Status](https://travis-ci.org/michaelmior/NoSE.svg?branch=master)](https://travis-ci.org/michaelmior/NoSE)
[![Dependency Status](https://gemnasium.com/michaelmior/NoSE.svg)](https://gemnasium.com/michaelmior/NoSE)

This is a work in progress tool to provide automated physical schema design for NoSQL data stores.
NoSE is licensed under the [GPLv3 license](LICENSE.md).

## Getting Started

If you want to quickly try NoSE, you can get a shell with all necessary dependencies using [Docker](https://www.docker.com/) as follows

    docker run --interactive --tty --rm michaelmior/nose /bin/bash

For continued use installing NoSE locally is more flexible and can be accomplished following the instructions below.

 * [Ruby](https://www.ruby-lang.org/) 2+
 * [bundler](http://bundler.io/)
 * [Cbc](https://projects.coin-or.org/Cbc) solver (see the [Dockerfile](Dockerfile) for packages on Ubuntu, [Homebrew](https://github.com/coin-or-tools/homebrew-coinor) maybe useful on Mac OS, but has not been tested)

Once dependencies have been installed, clone the repository and install the necessary Ruby gems

    git clone https://github.com/michaelmior/NoSE.git
    cd NoSE
    bundle install --without=development mysql

Examples of the workload input format is given in the `workloads/` directory.
These workloads should give you a sense of the input format and can be a starting point for your own workloads.
For example, to run the schema advisor against the workload `rubis`, simply execute the command below

    bundle exec nose search rubis

If you are prompted, accept the default configuration.
Each recommended physical structure is referred to as an "index" and will be the first set of outputs.
These indexes will be followed by a list of plans for each query which makes use of these indexes.
More information on the other commands available can be found with `bundle exec nose help`.
If you have any questions, please [open an issue](https://github.com/michaelmior/NoSE/issues/new) or contact [@michaelmior](https://github.com/michaelmior/).

## Development

Testing has been done with Ruby 2+ but most of the code should also run under the latest [JRuby](http://jruby.org/).
However, under JRuby, any code depending on C extensions or MRI internals should be excluded with `--without=development mysql`.

All source code is documented and more details on the command line tool can be retrieved by running `bundle exec nose help`.
You can view complete documentation by running `bundle exec rake doc` and viewing the output in the `doc/` directory.
Tests are written using [RSpec](http://rspec.info/) and can be executed with `bundle exec rspec`.
If you do not have a copy of the Cbc solver available, you can exclude tests depending on it with `--tag ~solver`.

Some commands require a configuration file in lieu of command line options.
An example configuration file for the different components of NoSE is given in [nose.yml.example](nose.yml.example).
Unfortunately, the line between what is configured in the configuration file and command line flags is currently somewhat blurry.

## Publications

Mior, M.J.; Kenneth Salem; Ashraf Aboulnaga; Rui Liu, [NoSE: Schema Design for NoSQL Applications](https://www.researchgate.net/publication/296485511_NoSE_Schema_Design_for_NoSQL_Applications), in Data Engineering (ICDE), 2016 IEEE 32nd International Conference on, 16-20 May 2016 (to appear)

![ACM DL Author-ize service](http://dl.acm.org/images/oa.gif) Michael J. Mior. 2014. [Automated schema design for NoSQL databases](http://dl.acm.org/authorize?N71145). In Proceedings of the 2014 SIGMOD PhD symposium (SIGMOD'14 PhD Symposium). ACM, New York, NY, USA, 41-45.
