What works?
* script to export query, DDL, query plan and statistics from the target database.
  * When a test query is provided, we export DDL and statistics only for tables used in the query.
* script to import statistics to a test database.
* Tests
  * Python test in this project to test the framework against benchmarks. Test results with TPCDS are as follows
    * When target and test universe have same size, we can match query plans with/without CBO enabled.
    * When target is 3 node universe and test is a single node universe, query plans match without CBO. With CBO enabled, selectivity estimates seems wrong.

What needs to be done?
* Why query plans don't match when target and test universe are different.
* Using the scripts in TAQO test framework @Dmitry
* Exporing GUCs and gflags.

Important points for Documentation
* Requirements for using the platform. 
  * Authentication
  * User Privileges for exporting/importing stats.
* Explain to the customer that user data may be exported as part of statistics.
  * Hints on how to sanitize data.

Open Questions
* Name for the feature.
* Authentication. Is user/password enough to connect? Do we need to support SSO?
* Namespace and Schema
  * When query is not provided, we want to export statistics for all tables. Currently though we export only statistics from the public namespace.

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
  