FROM ubuntu:16.04
MAINTAINER Michael Mior <mmior@uwaterloo.ca

RUN apt-get update -qq && \
    apt-get install -qq \
      build-essential \
      coinor-libcbc3 \
      coinor-libcbc-dev \
      coinor-libcgl-dev \
      coinor-libclp-dev \
      coinor-libcoinutils-dev \
      coinor-libosi-dev \
      graphviz \
      libmagickwand-dev \
      libmysqlclient-dev \
      ruby2.3 \
      ruby2.3-dev \
    && apt-get clean
RUN gem2.3 install bundler

ADD . /nose
WORKDIR /nose
RUN bundle install
