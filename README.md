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
| `gflags.csv` | Relevant gFlags that have been overridden from default |
| `ddl.sql` | DDL for the object used in the query. |
| `statistics.json` | Relevant information from pg_statistic and pg_class in JSON format |
| `query.sql` | The same query which was provided to the script. |
| `query_plan.txt` | Query plan generated on the customer system | 

### Limitations
* The script currently does not extract GUCs and gflags which may affect the 
query planner. 
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

### Test setup

The Yugabyte engineers can use the data exported from the customer deploy

The support engineers need to create a test environment using the same software
version and similar gflags as the customer deployment. The tables need to be 
created using the DDL exported from the customer.

### Importing the statistics

The `import_query_planner_stats.py` script can be used to import the statistics
from the JSON file.

```
usage: 
    import_query_planner_stats [-h] 
        [-H HOST] [-P PORT] 
        -D DATABASE -u USER [-p PASSWORD] 
        -s STAT_FILE
```

After importing the statistics, the query plan should be reproducible on the 
test system.


