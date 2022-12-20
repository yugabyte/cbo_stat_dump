# Benchmark test

The objective of this test is to test the frameworks against benchmarks.

```
$ python ./test/test_benchmark.py --help
usage: Test the framework to reproduce query plans on benchmarks

options:
  -h, --help            show this help message and exit
  -b BENCHMARK, --benchmark BENCHMARK
                        Name of the benchmark
  --target_host TARGET_HOST
                        Hostname or IP address, default localhost
  --target_port TARGET_PORT
                        Port number, default 5433
  --target_user TARGET_USER
                        YugabyteDB username, default yugabyte
  --target_password TARGET_PASSWORD
                        Password, default no password
  --target_database TARGET_DATABASE
                        Target Database name, default benchmark name with "_db" suffix
  --test_host TEST_HOST
                        Hostname or IP address, default same as TARGET_HOST
  --test_port TEST_PORT
                        Port number, , default same as TARGET_PORT
  --test_user TEST_USER
                        YugabyteDB username, default same as TARGET_USER
  --test_password TEST_PASSWORD
                        Password, default same as TARGET_PASSWORD
  --ignore_ran_tests, --no-ignore_ran_tests
                        Ignore tests for which an outdir exists
  --enable_optimizer_statistics, --no-enable_optimizer_statistics
                        Set yb_enable_optimizer_statistics=ON before running explain on query
```

Terminology: 
* TARGET is the dummy customer deployment with real dataset
* TEST is the test deployment where statistics will be imported

Credentials for the target and test instance can be provided as command line 
parameters. For purpose of testing, the target and test may point to the same 
instance. If the credentials for TEST deployment not provided, we will use the
TARGET deployment for testing.

The test can be run with minimum command line parameters as follows.

```
$ python -b tpcds
```

The test expects the TARGET deployment to contain a database with name 
`tpcds_db` with the data set and assumes that ANALYZE has been run on the 
tables. Further it expects the test queries to be in the path 
`./test/tpcds_queries`.

For each query in the benchmark suite, the test script will first run the 
`export_query_plan_data.py` script to extract the DDL, query plan and 
statistics from the TARGET database. Then it will create a new database 
on the TEST instance. The DDL will be run on the test database and statistics 
will be imported. The query plan from the test deployment will be extracted
and compared against the query plan from the target deployment.