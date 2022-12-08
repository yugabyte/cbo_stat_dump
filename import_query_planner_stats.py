#!/usr/bin/env python3

'''
export_query_planner_data utility
'''

import psycopg2
import argparse
import time
import os, platform, subprocess, sys
from pathlib import Path
import shutil
import re

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

def load_query_planner_sim_extension(cursor):
    query = """
        CREATE EXTENSION IF NOT EXISTS query_planner_sim;
    """
    cursor.execute(query)

def unload_query_planner_sim_extension(cursor):
    query = """
        DROP EXTENSION IF EXISTS query_planner_sim;
    """
    cursor.execute(query)

def import_statistics(cursor, stat_file_name):
    load_query_planner_sim_extension(cursor)
    if not os.path.exists(stat_file_name):
        print ("Statistics file %s does not exist" % stat_file_name)
    print ("Importing statistics from file %s" % stat_file_name)
    query = "select import_statistics_from_file('%s')" % stat_file_name
    cursor.execute(query)    
    unload_query_planner_sim_extension(cursor)

def main():
    args = parse_cmd_line()
    connectionDict = get_connection_dict(args)
    conn, cursor = connect_database(connectionDict)
    import_statistics(cursor, args.stat_file)

if __name__ == "__main__":
    main()