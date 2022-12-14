What works?
* When a test query is provided, we export DDL and statistics only for tables used in the query.
* Exporting/Importing statistics to/from JSON.
* For a majority of the queries from tpcds benchmark we are able to reproduce the query plan, but in some cases costs are not accurate.

What needs to be done?
* Figure out the cause for the difference in the query plans.
  * Accuracy loss in data type conversions between Postgres and JSON?
* Exporing GUCs and gflags.
  * Format? How to import?
  * yb_enable_optimizer_statistics
* Namespace and Schema
  * When query is not provided, we want to export statistics for all tables. Currently though we export only statistics from the public namespace.
    * ysql_dump should export DDL for all tables and schemas. We could use this to get the names of the tables to export statistics for.
* Automated tests.
  * Test against benchmark using TAQO.
  * Funtional tests in this project.
* Ability to simulate a large universe on a small test system.
* Reduce data
  * We can export statistics only for the columns that are used.
* Extended Statistics
  * Features like HyperLogLog statistics will need enhancements.

Important points for Documentation
* Requirements for using the platform. 
  * Authentication
  * User Privileges for exporting/importing stats.
* Explain to the customer that user data may be exported as part of statistics.
  * Hints on how to sanitize data.

Open Questions
* Name for the feature.
* Authentication. Is user/password enough to connect? Do we need to support SSO?

Name for the feature

* Aim is to reproduce query plans.
* Names from other vendors.
  * dump_stat - PostgresPro
  * minirepro - Greenplum
  * CBO Injector - Exasol
* Primary requirement is to export and import statistics and configuration parameters.
* Other use cases are testing.
* Options
  * CBO Reproducer -
  * Query Plan Simulator - 
  * QueryQuery - 
  * import_stats/export_stats
  * 

