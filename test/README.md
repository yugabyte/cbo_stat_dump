# Benchmark test

The objective of this test is to test the frameworks against benchmarks.

```
usage: 
    test_with_benchmark 
        [-h] 
        -b BENCHMARK 
        [--target_host TARGET_HOST] [--target_port TARGET_PORT] 
        [--target_user TARGET_USER] [--target_password TARGET_PASSWORD] 
        [--target_database TARGET_DATABASE] 
        [--test_host TEST_HOST] [--test_port TEST_PORT] 
        [--test_user TEST_USER] [--test_password TEST_PASSWORD]
        [--ignore_ran_tests] 
        [--enable_optimizer_statistics]
```

Terminology: 
* TARGET is the dummy customer deployment with real dataset
* TEST is the test deployment where statistics will be imported

Credentials for the target and test instance can be provided as command line 
parameters. For purpose of testing, the target and test may point to the same 
instance.

If credentials for TARGET are not provided the test tries to connect to the 
server on localhost:5433 with username `yugabyte` without password. If the 
credentials for TEST deployment not provided, the script uses the TARGET for 
testing.

The test expects the TARGET deployment to contain a database with name 
`<benchmark>_db`. This can be overridden by `--target_database`. The target 
database should contain the tables with data and ANALYZE should have been run on 
the tables. Further the script expects the test queries to be in the path 
`PROJECT_ROOT/test/<benchmark>_queries`.

For each query in the benchmark suite, the test script will first run the 
`cbo_stat_dump` script to extract the DDL, query plan and 
statistics from the TARGET database. Then it will create a new database 
on the TEST instance. The DDL will be run on the test database and statistics 
will be imported using the `cbo_stat_load` script. The query plan from the test 
deployment will be extracted and compared against the query plan from the target 
deployment.