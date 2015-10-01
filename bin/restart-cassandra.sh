#!/bin/bash

# Get the first data directory (we assume there's only one)
# The backup directory is just this directory with -bk appended
DATA_DIR=`grep data_file_directories -A 1 /etc/cassandra/cassandra.yaml | \
          tail -n +2 | sed 's/^\s\+-\s\+//; s/\/[^/]*$//'`

# Ideally sudo should be usable without a password so this can be automated
# We simply stop Cassandra, restore old data from a backup and restart
time sudo sh -c "service cassandra stop; \
                 rm -rf $DATA_DIR; \
                 cp -r $DATA_DIR-bk $DATA_DIR; \
                 chown -R cassandra:cassandra $DATA_DIR; \
                 service cassandra start"

until cqlsh `hostname -i` -e "USE $1"
do
  echo 'Waiting for Cassandra...'
  sleep 5
done
