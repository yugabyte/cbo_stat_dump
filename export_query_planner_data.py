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
from _ctypes import PyObj_FromPtr
import psycopg2
import argparse
import time
import os, platform, subprocess, sys
from pathlib import Path
import shutil
import re
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

QUERY_FILE_NAME = 'query.sql'
DDL_FILE_NAME = 'ddl.sql'
QUERY_PLAN_FILE_NAME = 'query_plan.txt'
STATISTICS_FILE_NAME = 'statistics.json'
PG_CLASS_FILE_NAME = 'pg_class.json'
DEFAULT_OUT_DIR_PREFIX = 'query_planner_data_'

STATISTICS_FETCH_ROWS_SIZE = 1000

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
    parser.add_argument('-o', '--out_dir', default='/tmp/' + DEFAULT_OUT_DIR_PREFIX +time.strftime("%Y%m%d-%H%M%S"), help='Output directory')
    parser.add_argument('-q', '--sql_file')

    args = parser.parse_args()
    return args

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

def get_process_output(cmd, output_file_name):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ)
    outmsg, errormsg = p.communicate()
    if p.returncode != 0:
        sys.stderr.writelines('\nError when executing the following command.\n\n' + cmd + '\n' + errormsg.decode() + '\n\n')
        sys.exit(1)
    with open(output_file_name, 'a') as output_file:
        output_file.write("%s" % outmsg.decode('UTF-8'))

def extract_ddl(cursor, connectionDict, relation_names, output_dir):
    if not shutil.which("ysql_dump"):
        sys.stderr.writelines("ysql_dump binary not found.\n\n")
        sys.exit(1)
    
    ddl_file_name = output_dir + '/' + DDL_FILE_NAME
    print("Exporting DDL to %s" % ddl_file_name)
    ddl_tmp_file_name = ddl_file_name + '.tmp'
    
    ysql_connection_str = "-d %s -h %s -p %s -U %s" % (connectionDict["database"], connectionDict["host"], connectionDict["port"], connectionDict["user"])
    if connectionDict["password"] is not None:
        ysql_connection_str = ysql_connection_str + "-W %s" % (connectionDict["password"])

    if relation_names:
        for relation_name in relation_names:
            ysql_dump_cmd = "ysql_dump %s -t %s -s" % (ysql_connection_str, relation_name)
            get_process_output(ysql_dump_cmd, ddl_tmp_file_name)
    else:
        ysql_dump_cmd = "ysql_dump %s -s" % (ysql_connection_str)
        get_process_output(ysql_dump_cmd, ddl_tmp_file_name)

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

# Custom JSON encoder to improve readability of statistics.json file
# Derived from https://stackoverflow.com/a/13252112
class NoIndent(object):
    """ Value wrapper. """
    def __init__(self, value):
        self.value = value

class CustomIndentEncoder(json.JSONEncoder):
    FORMAT_SPEC = '@@{}@@'
    regex = re.compile(FORMAT_SPEC.format(r'(\d+)'))

    def __init__(self, **kwargs):
        # Save copy of any keyword argument values needed for use here.
        self.__sort_keys = kwargs.get('sort_keys', None)
        super(CustomIndentEncoder, self).__init__(**kwargs)

    def default(self, obj):
        return (self.FORMAT_SPEC.format(id(obj)) if isinstance(obj, NoIndent)
                else super(CustomIndentEncoder, self).default(obj))

    def encode(self, obj):
        format_spec = self.FORMAT_SPEC  # Local var to expedite access.
        json_repr = super(CustomIndentEncoder, self).encode(obj)  # Default JSON.

        # Replace any marked-up object ids in the JSON repr with the
        # value returned from the json.dumps() of the corresponding
        # wrapped Python object.
        for match in self.regex.finditer(json_repr):
            # see https://stackoverflow.com/a/15012814/355230
            id = int(match.group(1))
            no_indent = PyObj_FromPtr(id)
            json_obj_repr = json.dumps(no_indent.value, sort_keys=self.__sort_keys)

            # Replace the matched id string with json formatted representation
            # of the corresponding Python object.
            json_repr = json_repr.replace(
                            '"{}"'.format(format_spec.format(id)), json_obj_repr)

        return json_repr

def export_statistics(cursor, relation_names, out_dir):
    statistics_file_name = out_dir + '/' + STATISTICS_FILE_NAME
    print("Exporting data from pg_statistic and pg_class to file %s" % statistics_file_name)

    relation_names_filter = ""
    if relation_names:
        relation_names_str = ', '.join(['\'{}\''.format(relation_name) for relation_name in relation_names])
        relation_names_filter = " and c.relname in (%s) " % relation_names_str

    statistics_dict = {}

    query = """
        SELECT row_to_json(t) FROM 
            (SELECT c.relname, c.reltuples, n.nspname
                FROM pg_class c JOIN pg_namespace n on c.relnamespace = n.oid and n.nspname = 'public' %s) t
        """ % (relation_names_filter);
    
    cursor.execute(query)
    rows_pg_class = cursor.fetchall()
    list_pg_class = []
    for row in rows_pg_class:
        list_pg_class.append(NoIndent(row[0]))
    statistics_dict['pg_class'] = list_pg_class

    # We fetch everything from pg_statistic except starelid and staattnum
    query = """
        SELECT row_to_json(t) FROM 
            (SELECT 
                c.relname relname, 
                a.attname attname, 
                t.typname typname,
                n.nspname nspname,
                s.stainherit,
                s.stanullfrac,
                s.stawidth,
                s.stadistinct,
                s.stakind1,
                s.stakind2,
                s.stakind3,
                s.stakind4,
                s.stakind5,
                s.staop1,
                s.staop2,
                s.staop3,
                s.staop4,
                s.staop5,
                s.stanumbers1,
                s.stanumbers2,
                s.stanumbers3,
                s.stanumbers4,
                s.stanumbers5,
                s.stavalues1,
                s.stavalues2,
                s.stavalues3,
                s.stavalues4,
                s.stavalues5
                FROM pg_class c 
                    JOIN pg_namespace n on c.relnamespace = n.oid and n.nspname = 'public' %s
                    JOIN pg_statistic s ON s.starelid = c.oid 
                    JOIN pg_attribute a ON c.oid = a.attrelid AND s.staattnum = a.attnum
                    JOIN pg_type t ON a.atttypid = t.oid) t
        """ % (relation_names_filter)

    cursor.execute(query)

    list_pg_statistic = []
    while True:
        pg_statistic_rows = cursor.fetchmany(STATISTICS_FETCH_ROWS_SIZE)
        if not pg_statistic_rows:
            break

        for row in pg_statistic_rows:
            list_pg_statistic.append(NoIndent(row[0]))

    statistics_dict['pg_statistic'] = list_pg_statistic
    statistics_json = json.dumps(statistics_dict, indent = 4, cls=CustomIndentEncoder)
    with open(statistics_file_name, 'w') as statistics_file:
        statistics_file.write(statistics_json)

def main():
    args = parse_cmd_line()
    connectionDict = get_connection_dict(args)
    conn, cursor = connect_database(connectionDict)
    Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    out_dir_abs_path = os.path.abspath(args.out_dir)
    relation_names = []
    if args.sql_file is not None:
        relation_names = get_relation_names_in_query(cursor, args.sql_file)
    #     export_query_file(args.sql_file, out_dir_abs_path)
    #     export_query_plan(cursor, args.sql_file, out_dir_abs_path)
    # extract_ddl(cursor, connectionDict, relation_names, out_dir_abs_path)
    export_statistics(cursor, relation_names, out_dir_abs_path)

if __name__ == "__main__":
    main()