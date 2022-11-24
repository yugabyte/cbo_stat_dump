#!/usr/bin/env python3
'''
export_query_planner_data utility

usage: export_query_planner_data [-h] [-u USERNAME] [-H HOST] [-P PORT] [-o OUT_DIR] [-q SQL_FILE]

Exports statistics and other data to reproduce query plan

options:
  -h, --help            show this help message and exit
  -u USERNAME, --username USERNAME
  -H HOST, --host HOST
  -P PORT, --port PORT
  -o OUT_DIR, --out_dir OUT_DIR
  -q SQL_FILE, --sql_file SQL_FILE
'''

import psycopg2
import argparse
import time
import os, platform, subprocess, sys
import json
from pathlib import Path
import shutil
import re

QUERY_FILE_NAME = 'query.sql'
DDL_FILE_NAME = 'ddl.sql'
QUERY_PLAN_FILE_NAME = 'query_plan.txt'
PG_STATISTIC_FILE_NAME = 'pg_statistic.json'
DEFAULT_OUT_DIR_PREFIX = 'query_planner_data_'

def parse_cmd_line():
    parser = argparse.ArgumentParser(
        prog = 'export_query_planner_data',
        description = 'Exports statistics and other data to reproduce query plan'
    )
    parser.add_argument('-u', '--user', required=True, help='YugabyteDB username')
    parser.add_argument('-p', '--password', help='Password')
    parser.add_argument('-H', '--host', help='Hostname or IP address')
    parser.add_argument('-P', '--port', help='Port number, default 5433')
    parser.add_argument('-D', '--database', required=True, help='Database name')
    parser.add_argument('-o', '--out_dir', default='/tmp/' + DEFAULT_OUT_DIR_PREFIX +time.strftime("%Y%m%d-%H%M%S"), help='Output directory')
    parser.add_argument('-q', '--sql_file')

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

def result_iter(cursor, arraysize=1000):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result

def json_search_relation_name_recurse(query_plan_json):
    relations = []
    if 'Plans' in query_plan_json:
        for sub_plan_json in query_plan_json['Plans']:
            if 'Relation Name' in sub_plan_json:
                relations.append(sub_plan_json['Relation Name'])
            relations = relations + (json_search_relation_name_recurse(sub_plan_json))
    return relations

def get_relation_names_in_query(cursor, sql_file):
    with open(sql_file, 'r') as sql_f:
        sql_text = sql_f.read()
    
    explain_query = "EXPLAIN (FORMAT JSON) %s" % sql_text
    cursor.execute(explain_query)
    query_plan_json = cursor.fetchone()[0]
    relations = []
    relations = json_search_relation_name_recurse(query_plan_json[0]['Plan'])
    return relations

def get_relation_oids(cursor, relation_names):
    relation_oids = []
    for relation_name in relation_names:
        oid_query = "SELECT oid FROM pg_class WHERE relname='%s'" % relation_name
        cursor.execute(oid_query)
        relation_oids.append(cursor.fetchone()[0])
    return relation_oids

def extract_ddl(cursor, connectionDict, relation_names, output_dir):
    ddl_file_name = output_dir + '/' + DDL_FILE_NAME
    print("Exporting DDL to %s" % ddl_file_name)
    ddl_tmp_file_name = ddl_file_name + '.tmp'
    
    for relation_name in relation_names:
        ysql_connection_str = "-d %s -h %s -p %s -U %s" % (connectionDict["database"], connectionDict["host"], connectionDict["port"], connectionDict["user"])
        if connectionDict["password"] is not None:
            ysql_connection_str = ysql_connection_str + "-W %s" % (connectionDict["password"])
        ysql_dump_cmd = "ysql_dump %s -t %s -s" % (ysql_connection_str, relation_name)
        # print (ysql_dump_cmd)
        p = subprocess.Popen(ysql_dump_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ)

        outmsg, errormsg = p.communicate()
        if p.returncode != 0:
            sys.stderr.writelines('\nError when executing function gp_dump_query_oids.\n\n' + errormsg.decode() + '\n\n')
            sys.exit(1)
        with open(ddl_tmp_file_name, 'w') as ddl_tmp_file:
            ddl_tmp_file.write("%s" % outmsg.decode('UTF-8'))

        with open(ddl_file_name, 'a') as ddl_file:
            with open(ddl_tmp_file_name, 'r') as ddl_tmp_file:
                for line in ddl_tmp_file:
                    if line.strip():
                        m = re.match(r"(?:^--)|(?:^SET)|(?:^SELECT pg_catalog)|(?:^ALTER TABLE [\w\.]+ OWNER TO)", line)
                        if m is None:
                            ddl_file.write(line)
            ddl_file.write('\n')

    
def export_query_file(sql_file, out_dir):
    query_file_name = out_dir + '/' + QUERY_FILE_NAME
    print("Exporting query file to %s" % query_file_name)
    shutil.copy(sql_file, query_file_name)

def export_query_plan(cursor, sql_file, out_dir):
    query_plan_file_name = out_dir + '/' + QUERY_PLAN_FILE_NAME
    print("Exporting query plan to %s" % query_plan_file_name)
    with open(sql_file, 'r') as sql_f:
        sql_text = sql_f.read()

    explain_query = "EXPLAIN %s" % sql_text
    cursor.execute(explain_query)
    query_plan = cursor.fetchall()
    with open(query_plan_file_name, 'w') as query_plan_file:
        for tuple in query_plan:
            query_plan_file.write(tuple[0] + '\n')

def export_pg_statistic(cursor, relation_names, out_dir):
    pg_statistic_file_name = out_dir + '/' + PG_STATISTIC_FILE_NAME
    print("Exporting pg_statistic to file %s" % pg_statistic_file_name)

    relation_names_str = ', '.join(['\'{}\''.format(relation_name) for relation_name in relation_names])
    query = """
        SELECT row_to_json(t) FROM 
            (SELECT 
                c.relname relname, 
                a.attname attname, 
                a.atttypid atttypid,
                s.stainherit,
                s.stanullfrac,
                s.stawidth,
                s.stadistinct,
                s.stakind1, s.stakind2, s.stakind3, s.stakind4, s.stakind5,
                quote_literal(to_schema_qualified_operator(s.staop1)) staop1_,
                quote_literal(to_schema_qualified_operator(s.staop2)) staop2_,
                quote_literal(to_schema_qualified_operator(s.staop3)) staop3_,
                quote_literal(to_schema_qualified_operator(s.staop4)) staop4_,
                quote_literal(to_schema_qualified_operator(s.staop5)) staop5_,
                s.stanumbers1, s.stanumbers2, s.stanumbers3, s.stanumbers4, s.stanumbers5,
                s.stavalues1, s.stavalues2, s.stavalues3, s.stavalues4, s.stavalues5,
                to_schema_qualified_type(anyarray_elemtype(s.stavalues1)) stavalues1_type,
                to_schema_qualified_type(anyarray_elemtype(s.stavalues2)) stavalues2_type,
                to_schema_qualified_type(anyarray_elemtype(s.stavalues3)) stavalues3_type,
                to_schema_qualified_type(anyarray_elemtype(s.stavalues4)) stavalues4_type,
                to_schema_qualified_type(anyarray_elemtype(s.stavalues5)) stavalues5_type
                FROM pg_class c 
                    JOIN pg_namespace n on c.relnamespace = n.oid and n.nspname = 'public' and c.relname in (%s)
                    JOIN pg_statistic s ON s.starelid = c.oid 
                    JOIN pg_attribute a ON c.oid = a.attrelid AND s.staattnum = a.attnum) t;
        """ % relation_names_str
    cursor.execute(query)
    statistics = cursor.fetchall()
    with open(pg_statistic_file_name, 'w') as pg_statistic_file:
        for tuple in statistics:
            pg_statistic_file.write(json.dumps(tuple[0]) + '\n')

def main():
    args = parse_cmd_line()
    connectionDict = get_connection_dict(args)
    conn, cursor = connect_database(connectionDict)
    Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    relation_names = []
    if args.sql_file is not None:
        relation_names = get_relation_names_in_query(cursor, args.sql_file)
        # export_query_file(args.sql_file, args.out_dir)
        # extract_ddl(cursor, connectionDict, relation_names, args.out_dir)
        # export_query_plan(cursor, args.sql_file, args.out_dir)
    export_pg_statistic(cursor, relation_names, args.out_dir)

if __name__ == "__main__":
    main()