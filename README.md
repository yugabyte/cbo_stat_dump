# Introduction

The python scripts in this project can be used to reproduce query planning 
issues without having to replicate the data or the cluster.

The role of the query planner is to find the most efficient execution plan for a
query. The key idea is that by setting a test environment and feeding the query
planner with information extracted from the customer environment, we can 
accurately reproduce query plans. 

## Exporting information from customer deployment

The `cbo_stat_dump.py` script is used to extract information from a 
customer deployment. 

```
usage: 
    cbo_stat_dump [-h] 
        [-H HOST] [-P PORT] -D DATABASE
        -u USER [-p PASSWORD] 
        [-o OUT_DIR] [-q SQL_FILE] 
        [--enable_optimizer_statistics]
```

This script can be run on a client and will connect to the customer database 
using credentials provided in command line arguments. Note that the user must 
have appropriate privileges to access system tables and objects used in the 
query. The script will export the information in the `<OUT_DIR>` or in a folder 
in `/tmp` on the client where the script is run. The following information is 
exported,

| File name | Description |
| --------- | ----------- |
| `version.txt` | YugabyteDB version |
| `ysql_pg_conf.csv` | Relevant GUC that have been overridden from default | 
| `gflags.json` | Relevant gFlags that have been overridden from default |
| `ddl.sql` | DDL for the object used in the query. |
| `statistics.json` | Relevant information from pg_statistic and pg_class in JSON format |
| `query.sql` | The same query which was provided to the script. |
| `query_plan.txt` | Query plan generated on the customer system | 

### Limitations
* The script is currently unable to extract CREATE statements for UDFs that may 
be used in the query. 

### Important to note

The `statistics.json` file may contain sensitive information with samples of the
customer data. This should be explained to the customer. The customer may choose
to sanitize this data by removing statistics for columns which are not used in 
the query or strategically modifying the data such that the interpretation of
the statistics does not change significantly. For example, some low significance
digits in credit card numbers can be changed.

## Reproducing the query plan

The exported data can be used by Yugabyte engineers to reproduce the query plan
using the following steps.

1. Create a test cluster with a debug build of the same software version as 
the customer. Configure GUCs and gflags in the same way as the customer. These 
can be found from the following files,
    * `version.txt`
    * `ysql_pg_conf.csv`
    * `gflags.json`

> **Note**: As of now, the size and topology of the cluster do not affect the query
plan. So the test cluster need not have the same topology as the customer. This 
may change in future.

2. Create the schema with empty tables using the `ddl.sql` file.

3. Load statistics from `statistics.json` using the script `cbo_stat_load` as 
follows.
```
usage: 
    cbo_stat_load [-h] 
        [-H HOST] [-P PORT] 
        -D DATABASE -u USER [-p PASSWORD] 
        -s STAT_FILE
```

4. Run the query in `query.sql` with `EXPLAIN` and the customers query plan in 
`query_plan.txt` can be accurately reproduced.
