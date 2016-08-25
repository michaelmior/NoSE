# RUBiS Experiments

This directory contains instructions and various scripts for running a performance analysis on different RUBiS schemas.
Currently these experiments are run against the Cassandra backend using the MySQL loader to populate the column families.
You will need to configure a Cassandra cluster with a keyspace named `rubis` and a MySQL cluster with a database named `rubis`.
Once this is done, initialize `nose.yml` in the root of the repository with the configuration below.
Note that you will need to edit the configuration with the correct connection information for Cassandra and MySQL.

```yaml
backend:
  name: cassandra
  hosts:
    - 10.0.0.2
  port: 9042
  keyspace: rubis
cost_model:
  name: cassandra

  index_cost: 0.0078395645
  partition_cost: 0.0013692786
  row_cost: 1.17093638386496e-005
  delete_cost: 0.0013287903
  insert_cost: 0.013329108
loader:
  name: mysql
  directory: /tmp/csv
  host: 127.0.0.1
  database: rubis
  username: root
  password: root
```

First create the RUBiS schema in MySQL.

    mysql -uroot -proot -Drubis < rubis-schema.sql
    mysql -uroot -proot -Drubis < rubis-update.sql

To populate the MySQL database with some test data, we use the [mysql-faker](https://www.npmjs.com/package/mysql-faker) Node.js package.
This package does not use the MySQL configuration in `nose.yml` so it may need to be manually edited.
Next, install mysql-faker and populate the database.

    npm install
    node fake.js

Once this script finishes, we are ready to load data in Cassandra.
At this point, you can use either one of the manually-defined schemas, `rubis_baseline` or `rubis_expert` or use a JSON results file output by `nose search`.
We refer to the choice of schema to use as `SCHEMA` for the remainder of the instructions.
Now we can create the Cassandra column families and load the data from MySQL.
This step may take several hours to complete.

    bundle exec nose create SCHEMA
    bundle exec nose load SCHEMA

Since the experiments are destructive (i.e. they modify data in the database), it's a good idea to [take a snapshot](https://docs.datastax.com/en/cassandra/2.0/cassandra/operations/ops_backup_restore_c.html) before continuing.
Finally, experiments can be run using `nose execute` for a manually-defined schema or `nose benchmark` for a schema generated with `nose search`.

## Running multiple experiments

As mentioned above, experiments are destructive since updates modify the populated data.
The easiest way to run multiple experiments is to take a snapshot after populating the data but before running the first experiment.

    nodetool snapshot rubis -t SNAPSHOT_NAME

The script below will restore the snapshot at which point you will be ready to run another experiment.
Be sure to replace `SCHEMA` and `SNAPSHOT_NAME` with the appropriate values.

```bash
# Drop and recreate all tables
for cf in $(cqlsh 10.0.0.2 -k rubis -f <(echo 'DESCRIBE COLUMNFAMILIES') | tr ' ' '\n' | grep -Ev '^$'); do
  cqlsh 10.0.0.2 -k rubis -f <(echo "DROP TABLE $cf;")
done

bundle exec nose create SCHEMA

# Restore snapshot
for ssdir in $(find /ssd1/mmior/cassandra/data/rubis_big/ -wholename '*/snapshots/SNAPSHOT_NAME' -type d); do
  for file in $(find "$ssdir/" -type f | rev | cut -d/ -f1 | rev); do
    sudo ln "$ssdir/$file" "$ssdir/../../$file"
  done
done

# Refresh column families
for cf in $(cqlsh 10.0.0.2 -k rubis -f <(echo 'DESCRIBE COLUMNFAMILIES') | tr ' ' '\n' | grep -Ev '^$'); do
  nodetool refresh rubis $cf
done
```
