# Introduction

The `cbo_stat_dump` script can be used to reproduce complex query planning
issues encoundered in production environment running PostgreSQL or YugabyteDB.

Debugging these issues in production environment could be very painful because
of the complexity of the query planner and limited observability. Developers
would need access to production environments which may contain sensitive data
and debugging operations may affect active production use cases.

To pick an optimal execution plan, the Cost Based Optimizer aka. query planner
uses table and column statistics to estimate the cost of executing a query. By
replicating the schema and statistics in a test environment, we can force the
query planner to make the same plan choice as in a large production environment,
without having to replicate the actual production data.

By being able to easily reproduce issues, developers will be able efficiently
debug the problem and provide effective workarounds and resolutions.

The `cbo_stat_dump` script exports all the information needed to reproduce
a query planning issue. This primarily includes,
* GUC parameters
* Schema Definition
* Statistics

## Note

No changes are needed in production environments to use this script. However 
some patches may be needed in the test environment to import statistics.

### Force query planner to rely only on `reltuples` estimate in `pg_class`

In PostgreSQL statistics are updated periodically. In between, statistics may
become stale, which can lead to incorrect plan choices. In particular the size
of the table may change rapidly and `reltuples` in `pg_class` may be out-dated.
To mitigate this, a feature was implemented in PostgreSQL version 15 and onwards
which estimates the number of tuples in a table by extrapolating `reltuples`
from `pg_class` by checking the actual number of pages used by the table in
the storage.

In our test system, tables will be empty, so query planner would get an
estimated row count of 0. A patch is needed to force the query planner to
rely on the `reltuples` estimate in `pg_class`.

The patch 
`postgres_patches/master/0002-Ignore-relpages-estimate-from-storage-manager.patch`
needs to be applied to vanilla PostgreSQL code to use on test systems.
Additionally you must set the `enable_cbo_simulation` GUC to force the
query planner to rely on `reltuples` in `pg_class`.

Despite this patch, we may not be able to reproduce the exact same row
estimates and costs as in production environment, however in most cases we
should get the same execution plans as long as the statistics are not too 
out-dated.

This patch is not needed on earlier versions of PostgreSQL or YugabyteDB.

### Extended Statistics

Vanilla PostgreSQL does not allow inserting data to `pg_statistics_ext_data`
system table. 

If extended statistics are used in production envrironment, you must apply
the patch 
`postgres_patches/master/0001-Support-inserting-statistics-to-pg_statistic_ext_dat.patch`
to allow inserting data to the extended statistics table. This patch is also
needed in YugabyteDB.

## Exporting information from customer deployment

The `cbo_stat_dump.py` script is used to extract information from a 
customer deployment. 

```
python3.13 ./cbo_stat_dump --help                                    

usage: cbo_stat_dump [--help] [--debug] [-h HOST] [-p PORT] [-d DATABASE] [-s SCHEMAS] [-u USERNAME] [-W PASSWORD] [-o OUT_DIR] [-q QUERY_FILE]
                     [--yb_mode] [--enable_base_scans_cost_model | --no-enable_base_scans_cost_model]

Exports statistics and other data to reproduce query plan

options:
  --help                show this help message and exit
  --debug               Set log level to DEBUG
  -h, --host HOST       Hostname or IP address, default localhost
  -p, --port PORT       Port number
  -d, --database DATABASE
                        Database name
  -s, --schemas SCHEMAS
                        Comma separated list of schema names. Use quotes to include spaces in schema names, default all schemas apart from pg_catalog,
                        pg_toast, and information_schema
  -u, --username USERNAME
                        Username
  -W, --password PASSWORD
                        Password, default no password
  -o, --out_dir OUT_DIR
                        Output directory
  -q, --query_file QUERY_FILE
                        File containing query that needs to be debugged
  --yb_mode             Use this mode to export data from YugabyteDB
  --enable_base_scans_cost_model, --no-enable_base_scans_cost_model
                        Set yb_enable_base_scans_cost_model=ON before running explain on query
```

This script will connect to the customer database using credentials provided in 
command line arguments. The user must have appropriate privileges to access system 
tables and objects used in the query. The script will export the information in 
the `<OUT_DIR>` or in a folder in `/tmp` on the client where the script is run. 
The following information is exported,

| File name | Description |
| --------- | ----------- |
| `version.txt` | PostgreSQL or YugabyteDB version |
| `overridden_gucs.csv` | Relevant GUC that have been overridden from default | 
| `gflags.json` | Relevant YugabyteDB gFlags that have been overridden from default |
| `ddl.sql` | DDL for the object used in the query. |
| `statistics.json` | Relevant information from pg_statistic and pg_class in JSON format |
| `statistics_ext.json` | Relevant information from pg_statistic and pg_class in JSON format |
| `import_statistics.sql` | Relevant information from pg_statistic and pg_class in JSON format |
| `import_statistics_ext.sql` | Relevant information from pg_statistic and pg_class in JSON format |
| `query.sql` | The query optionally provided using `-q` option. |
| `query_plan.txt` | Query plan for the query on the target instance. | 

### Limitations
* The script is currently unable to extract CREATE statements for UDFs that may 
  be used in the query.
* `pg_dump` does not export `CREATE STATISTICS` statements when extracting DDL for
  individual tables. This functionality is used when user specifies a query using
  '-q' option, to extract DDL only for tables used in the query. These statements
  will need to be manually collected in this case.

### Important to note

The `statistics.json` file may contain sensitive information with samples of the
customer data. This should be explained to the customer. The customer may choose
to sanitize this data by removing statistics for columns which are not used in 
the query or strategically modifying the data such that the interpretation of
the statistics does not change significantly. For example, some low significance
digits in credit card numbers can be changed.

## Steps to reproduce production environment

1. Create a debug build with the version in `version.txt`, along with the
included patch to enable inserting statistics to system catalog tables.

2. Create a test cluster with the debug build. Configure GUCs as the customer. These 
can be found from the following files,
    * `overridden_gucs.sql`

3. Create the schema with empty tables using the `ddl.sql` file.

4. Load statistics by running `import_statistics.sql` and `import_statistics_ext.sql`.
