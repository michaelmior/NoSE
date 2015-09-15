#!/bin/sh

eval `bundle exec nose export`

# The first argument should be the directory where results are stored
RESULTS_DIR=$1
REPEAT=1
ITERATIONS=1000
COMMON_OPTIONS="--num-iterations=$ITERATIONS --repeat=$REPEAT --format=csv"

# Enable command output and fail on error
set -e
set -x

mkdir -p $RESULTS_DIR

# Passwordless SSH access must be set up to the backend host
restart_cassandra() {
  ssh $BACKEND_HOSTS_0 `pwd`/bin/restart_cassandra.sh $BACKEND_KEYSPACE
}

run_nose_search() {
  bundle exec nose search rubis --format=json --mix=$1 > $RESULTS_DIR/$1.json
}

run_nose_search bidding
restart_cassandra

bundle exec nose benchmark $COMMON_OPTIONS --mix=bidding \
  $RESULTS_DIR/bidding.json > $RESULTS_DIR/bidding.csv

run_nose_search write_heavy
restart_cassandra

bundle exec nose benchmark $COMMON_OPTIONS --mix=write_heavy \
  $RESULTS_DIR/write_heavy.json > $RESULTS_DIR/write_heavy.csv

restart_cassandra

bundle exec nose benchmark $COMMON_OPTIONS --mix=write_heavy \
  $RESULTS_DIR/bidding.json > $RESULTS_DIR/bidding_write_heavy.csv

restart_cassandra

bundle exec nose execute $COMMON_OPTIONS --mix=bidding \
  rubis_expert > $RESULTS_DIR/expert.csv

restart_cassandra

bundle exec nose execute $COMMON_OPTIONS --mix=write_heavy \
  rubis_expert > $RESULTS_DIR/expert_write_heavy.csv

restart_cassandra

bundle exec nose execute $COMMON_OPTIONS --mix=bidding \
  rubis_baseline > $RESULTS_DIR/baseline.csv

restart_cassandra

bundle exec nose execute $COMMON_OPTIONS --mix=write_heavy \
  rubis_baseline > $RESULTS_DIR/baseline_write_heavy.csv
