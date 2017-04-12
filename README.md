# NoSQL Schema Evaluator (NoSE)

[![Build Status](https://travis-ci.org/michaelmior/NoSE.svg?branch=master)](https://travis-ci.org/michaelmior/NoSE)
[![Dependency Status](https://gemnasium.com/michaelmior/NoSE.svg)](https://gemnasium.com/michaelmior/NoSE)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/michaelmior/NoSE/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/michaelmior/NoSE/?branch=master)
[![Code Coverage](https://scrutinizer-ci.com/g/michaelmior/NoSE/badges/coverage.png?b=master)](https://scrutinizer-ci.com/g/michaelmior/NoSE/?branch=master)
[![Docker Build Status](https://img.shields.io/docker/build/michaelmior/nose.svg)]()

This is a work in progress tool to provide automated physical schema design for NoSQL data stores.
NoSE is licensed under the [GPLv3 license](LICENSE.md).

## Getting Started

If you want to quickly try NoSE, you can get a shell with all necessary dependencies using [Docker](https://www.docker.com/) as follows

    docker run --interactive --tty --rm michaelmior/nose /bin/bash

For continued use, installing a development version of the NoSE CLI is more flexible.
Instructions can be found in the [nose-cli](https://github.com/michaelmior/nose-cli) repository.

## Publications

Mior, M.J.; Kenneth Salem; Ashraf Aboulnaga; Rui Liu, [NoSE: Schema Design for NoSQL Applications](https://www.researchgate.net/publication/296485511_NoSE_Schema_Design_for_NoSQL_Applications), in Data Engineering (ICDE), 2016 IEEE 32nd International Conference on, 16-20 May 2016 (to appear)

![ACM DL Author-ize service](http://dl.acm.org/images/oa.gif) Michael J. Mior. 2014. [Automated schema design for NoSQL databases](http://dl.acm.org/authorize?N71145). In Proceedings of the 2014 SIGMOD PhD symposium (SIGMOD'14 PhD Symposium). ACM, New York, NY, USA, 41-45.

## Acknowledgements

This work was supported by the Natural Sciences and Engineering Research Council of Canada ([NSERC](http://nserc.gc.ca)).

[![NSERC](assets/NSERC-logo.png)](http://nserc.gc.ca)

Hosting of [Coin-OR packages](https://packagecloud.io/michaelmior/coinor/) is generously provided by packagecloud.

[![packagecloud](assets/packagecloud-logo.png)](https://packagecloud.io)
