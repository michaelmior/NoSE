# NoSQL Schema Evaluator (NoSE)

[![Build Status](https://travis-ci.org/michaelmior/NoSE.svg?branch=master)](https://travis-ci.org/michaelmior/NoSE)
[![Depfu](https://badges.depfu.com/badges/69de42ee3415b077a040beadc8941f1e/overview.svg)](https://depfu.com/github/michaelmior/NoSE?project_id=6964)
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

Mior, M.J.; Kenneth Salem; Ashraf Aboulnaga; Rui Liu, [NoSE: Schema Design for NoSQL Applications](https://www.researchgate.net/publication/296485511_NoSE_Schema_Design_for_NoSQL_Applications), in Data Engineering (ICDE), July 2017.

Mior, M.J.; Kenneth Salem; Ashraf Aboulnaga; Rui Liu, [NoSE: Schema Design for NoSQL Applications](https://www.researchgate.net/publication/318126769_NoSE_Schema_Design_for_NoSQL_Applications), Transactions on Knowledge and Data Engineering, 16-20 May 2016.

![ACM DL Author-ize service](http://dl.acm.org/images/oa.gif) Michael J. Mior. 2014. [Automated schema design for NoSQL databases](http://dl.acm.org/authorize?N71145). In Proceedings of the 2014 SIGMOD PhD symposium (SIGMOD'14 PhD Symposium). ACM, New York, NY, USA, 41-45.

## Acknowledgements

This work was supported by the Natural Sciences and Engineering Research Council of Canada ([NSERC](http://nserc.gc.ca)).

[![NSERC](assets/NSERC-logo.png)](http://nserc.gc.ca)

Hosting of [Coin-OR packages](https://packagecloud.io/michaelmior/coinor/) is generously provided by packagecloud.

[![packagecloud](assets/packagecloud-logo.png)](https://packagecloud.io)
