#!/usr/bin/env python3

'''
export_query_planner_data utility
'''

import psycopg2
import argparse
import os, platform
import json

def get_connection_dict(args):
    envOpts = os.environ
    host = args.host or platform.node();
    port = args.port or ('PGPORT' in envOpts and envOpts['PGPORT']) or '5433'
    user = args.user
    password = args.password
    db = args.database;
    print ("Connecting to database host=%s, port=%s, user=%s, db=%s" % (host, port, user, db))

    connectionDict = {
        'host': host,
        'port': port,
        'user': user,
        'password': password,
        'database': db
    }
    return connectionDict

def connect_database(connectionDict):
    conn = psycopg2.connect(**connectionDict)
    cursor = conn.cursor()
    return conn, cursor

def parse_cmd_line():
    parser = argparse.ArgumentParser(
        prog = 'export_query_planner_data',
        description = 'Exports statistics and other data to reproduce query plan'
    )
    parser.add_argument('-H', '--host', help='Hostname or IP address')
    parser.add_argument('-P', '--port', help='Port number, default 5433')
    parser.add_argument('-D', '--database', required=True, help='Database name')
    parser.add_argument('-u', '--user', required=True, help='YugabyteDB username')
    parser.add_argument('-p', '--password', help='Password')
    parser.add_argument('-s', '--stat_file', required=True, help='JSON file with table statistics')
    
    args = parser.parse_args()
    return args

def update_pg_statistic(cursor, stat_json):
    columnTypes = { "stainherit": "boolean",
                    "stanullfrac": "real",
                    "stawidth": "integer",
                    "stadistinct": "real",
                    "stakind1": "smallint",
                    "stakind2": "smallint",
                    "stakind3": "smallint",
                    "stakind4": "smallint",
                    "stakind5": "smallint",
                    "staop1": "oid",
                    "staop2": "oid",
                    "staop3": "oid",
                    "staop4": "oid",
                    "staop5": "oid",
                    "stanumbers1": "real[]",
                    "stanumbers2": "real[]",
                    "stanumbers3": "real[]",
                    "stanumbers4": "real[]",
                    "stanumbers5": "real[]"}

    stavaluesType = stat_json["typname"]
    if (stavaluesType.startswith('_')):
        stavaluesType += '[]'
    for i in range (1, 5):
        columnTypes["stavalues" + str(i)] = stavaluesType

    columnValues = ""
    for columnName, columnType in columnTypes.items():
        columnValues += ", " + str(stat_json[columnName]) + "::" + columnType        

    starelid = "'%s.%s'::regclass" % (stat_json["nspname"], stat_json["relname"])
    # Find staattnum from starelid and "attname" from statistics
    query = "SELECT a.attnum FROM pg_attribute a WHERE a.attrelid = %s and a.attname = '%s'" % (starelid, stat_json["attname"])
    cursor.execute(query)
    staattnum = cursor.fetchone()[0]
    query = """
        DELETE FROM pg_statistic WHERE starelid = %s AND attnum = %s;
        INSERT INTO pg_statistic VALUES (%s, %s%s)
        """ % (starelid, staattnum, starelid, staattnum, columnValues)

def update_reltuples(cursor, relnamespace, relname, reltuples):
    query = "UPDATE pg_class SET reltuples = %s WHERE relnamespace = '%s'::regnamespace AND (relname = '%s' OR relname = '%s_pkey')" % (reltuples, relnamespace, relname, relname)
    cursor.execute(query)
    # TODO: verify that 2 rows were updated.

def enable_write_on_sys_tables(cursor):
    query = "SET yb_non_ddl_txn_for_sys_tables_allowed = ON"
    cursor.execute(query)

def disable_write_on_sys_tables(cursor):
    query = "SET yb_non_ddl_txn_for_sys_tables_allowed = OFF"
    cursor.execute(query)

def import_statistics(cursor, stat_file_name):
    if not os.path.exists(stat_file_name):
        print ("Statistics file %s does not exist" % stat_file_name)
    print ("Importing statistics from file %s" % stat_file_name)

    statistics_json = json.load(open(stat_file_name, 'r'))

    enable_write_on_sys_tables(cursor)

    for pg_class_row_json in statistics_json['pg_class']:
        update_reltuples(cursor, pg_class_row_json["nspname"], pg_class_row_json["relname"], pg_class_row_json["reltuples"])

    for pg_statistic_row_json in statistics_json['pg_statistic']:
        print (pg_statistic_row_json["nspname"] + "." + pg_statistic_row_json["relname"] + "." + pg_statistic_row_json["attname"])
        update_pg_statistic(cursor, pg_statistic_row_json)
        
    disable_write_on_sys_tables(cursor)

def main():
    args = parse_cmd_line()
    connectionDict = get_connection_dict(args)
    conn, cursor = connect_database(connectionDict)
    import_statistics(cursor, args.stat_file)

if __name__ == "__main__":
    main()