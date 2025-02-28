# Introduction

The `cbo_stat_dump` script is used to extract all the information needed to
reproduce a complex query planning issue encountered on a production environment
running PostgreSQL. This information includes DDL, GUCs and statistics used by the 
query planner. By creating empty tables and importing the statistics in a test 
instance, we can force the query planner to make the same plan choices. This can
be used to effectively debug issues and provide reliable solutions.

The query planner uses certain table and column level statistics to perform
selectivity estimation. These statistics are available in system catalog tables
`pg_class`, `pg_statistic`, `pg_statistic_ext` and `pg_statistic_ext_data`. This
tool is able to extract the statistics from these tables. It exports this data
in JSON format, and also generates SQL statements to import these statistics
on a test instance.

Additionally the tool also exports DDL statements to recreate the table schema
and values of GUC parameters relevant to query planner that have been overridden
on the production environment.

## Note

While no changes are needed on production environments, inserting to system
catalog tables is not supported by default on vanilla PostgreSQL. PostgreSQL
also does not recommend writing to system catalog tables due the risk of
data corruption and unexpected outcomes. 

We may be able to tolerate these risks in a test environment. To allow inserting 
statistics in system catalog tables, a patch needs to be applied to PG on the 
test instance.

Moreover, an optimization included in PG15 onwards, make it harder to
reproduce plans precisely. Instead of using `reltuples` in `pg_class` the query 
planner takes the current number of blocks in the file containing the table and 
extrapolates the value of reltuples. This means than on target system the query
planner sees a different value of reltuples, than what is shown in `pg_class`.
On the test system since the tables are actually empty, the `query_planner` 
disregards the reltuples in pg_class and reports that the table has 0 rows.

The patch allows users to disable this optimization in test by enabling the GUC
`enable_cbo_simulation`.

This optimization may cause plans to be not exactly reproducible, because stats 
may be stale on the target instance. If stats are up-to date we should be able 
to reproduce the same query plans.

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
