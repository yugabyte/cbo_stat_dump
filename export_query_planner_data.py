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
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time

from _ctypes import PyObj_FromPtr
from pathlib import Path

import psycopg2

import urllib.request, json

def get_connection_dict(args):
    host = args.host
    port = args.port
    user = args.user
    password = args.password
    db = args.database
    print(f"Connecting to database host={host}, port={port}, user={user}, db={db}")

    return {
        'host': host,
        'port': port,
        'user': user,
        'password': password,
        'database': db
    }


def connect_database(connection_dict):
    conn = psycopg2.connect(**connection_dict)
    cursor = conn.cursor()
    return conn, cursor


QUERY_FILE_NAME = 'query.sql'
DDL_FILE_NAME = 'ddl.sql'
QUERY_PLAN_FILE_NAME = 'query_plan.txt'
STATISTICS_FILE_NAME = 'statistics.json'
YSQL_PG_CONF_FILE_NAME = 'ysql_pg_conf.csv'
GFLAGS_FILE_NAME = 'gflags.json'
PG_CLASS_FILE_NAME = 'pg_class.json'
DEFAULT_OUT_DIR_PREFIX = 'query_planner_data_'
CBO_RELEVANT_GUC_PARAMS = {'yb_enable_optimizer_statistics', 
                           'yb_bnl_batch_size', 
                           'yb_enable_expression_pushdown', 
                           'yb_enable_geolocation_costing', 
                           'yb_test_planner_custom_plan_threshold'}

STATISTICS_FETCH_ROWS_SIZE = 1000


def parse_cmd_line():
    parser = argparse.ArgumentParser(
        prog='export_query_planner_data',
        description='Exports statistics and other data to reproduce query plan'
    )
    parser.add_argument('-H', '--host', help='Hostname or IP address, default localhost',
                        default="localhost")
    parser.add_argument('-P', '--port', help='Port number, default 5433', default=5433)
    parser.add_argument('-D', '--database', required=True, help='Database name, default yugabyte',
                        default="yugabyte")
    parser.add_argument('-u', '--user', required=True, help='YugabyteDB username, default yugabyte',
                        default="yugabyte")
    parser.add_argument('-p', '--password', help='Password, default no password')
    parser.add_argument('-o', '--out_dir',
                        default='/tmp/' + DEFAULT_OUT_DIR_PREFIX + time.strftime("%Y%m%d-%H%M%S"),
                        help='Output directory')
    parser.add_argument('-q', '--sql_file')
    parser.add_argument('--enable_optimizer_statistics', action=argparse.BooleanOptionalAction,
                        help='Set yb_enable_optimizer_statistics=ON before running explain on query')

    args = parser.parse_args()
    return args


def get_relations_from_json_recurse(query_plan_json):
    relations = []
    if 'Plans' in query_plan_json:
        for sub_plan_json in query_plan_json['Plans']:
            if 'Relation Name' in sub_plan_json:
                relations.append(sub_plan_json['Relation Name'])
            relations = relations + (get_relations_from_json_recurse(sub_plan_json))
    return relations


def get_relation_names_in_query(cursor, sql_file):
    with open(sql_file, 'r') as sql_f:
        sql_text = sql_f.read()
    explain_query = f"EXPLAIN (FORMAT JSON) {sql_text}"

    try:
        cursor.execute(explain_query)
        query_plan_json = cursor.fetchone()[0][0]
    except Exception as e:
        print(f"Failed to evaluate:\n{explain_query}")
        raise e

    relations = []
    if 'Relation Name' in query_plan_json['Plan']:
        relations.append(query_plan_json['Plan']['Relation Name'])
    relations.extend(get_relations_from_json_recurse(query_plan_json['Plan']))
    relations = list(dict.fromkeys(relations))
    return relations


def get_relation_oids(cursor, relation_names):
    relation_oids = []
    for relation_name in relation_names:
        oid_query = f"SELECT oid FROM pg_class WHERE relname='{relation_name}'"
        cursor.execute(oid_query)
        relation_oids.append(cursor.fetchone()[0])
    return relation_oids


def get_process_output(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         env=os.environ)
    outmsg, errormsg = p.communicate()
    if p.returncode != 0:
        sys.stderr.writelines(
            '\nError when executing the following command.\n\n' + cmd + '\n' + errormsg.decode() + '\n\n')
        sys.exit(1)
    return outmsg.decode('UTF-8')


def extract_ddl(connection_dict, relation_names, output_dir):
    if not shutil.which("ysql_dump"):
        sys.stderr.writelines("ysql_dump binary not found.\n\n")
        sys.exit(1)
    
    ddl_file_name = f'{output_dir}/{DDL_FILE_NAME}'
    print("Exporting DDL to %s" % ddl_file_name)
    ddl_tmp_file_name = ddl_file_name + '.tmp'
    ysql_connection_str = "-d %s -h %s -p %s -U %s" % (connection_dict["database"], connection_dict["host"], connection_dict["port"], connection_dict["user"])

    password_prefix_str = ""
    if connection_dict["password"] is not None:
        password_prefix_str = f'PGPASSWORD={connection_dict["password"]} '
    if relation_names:
        for relation_name in relation_names:
            ysql_dump_cmd = password_prefix_str + ("ysql_dump %s -t %s -s" % (ysql_connection_str, relation_name))

            outmsg = get_process_output(ysql_dump_cmd)
            with open(ddl_tmp_file_name, 'a') as ddl_tmp_file:
                ddl_tmp_file.write(outmsg)
    else:
        ysql_dump_cmd = password_prefix_str + "ysql_dump %s -s" % ysql_connection_str
        outmsg = get_process_output(ysql_dump_cmd)
        with open(ddl_tmp_file_name, 'w') as ddl_tmp_file:
            ddl_tmp_file.write(outmsg)
    with open(ddl_file_name, 'w') as ddl_file:
        with open(ddl_tmp_file_name, 'r') as ddl_tmp_file:
            for line in ddl_tmp_file:
                if line.strip():
                    m = re.match("(?:^--)|(?:^SET)|(?:^SELECT pg_catalog)|(?:^ALTER TABLE [\w\.]+ OWNER TO)", line)

                    if m is None:
                        ddl_file.write(line)
        ddl_file.write('\n')
    os.remove(ddl_tmp_file_name)


def export_query_file(sql_file, out_dir):
    query_file_name = f'{out_dir}/{QUERY_FILE_NAME}'
    print(f"Exporting query file to {query_file_name}")
    shutil.copy(sql_file, query_file_name)


def export_query_plan(cursor, sql_file, out_dir, enable_optimizer_statistics):
    query_plan_file_name = f'{out_dir}/{QUERY_PLAN_FILE_NAME}'
    print(f"Exporting query plan to {query_plan_file_name}")
    with open(sql_file, 'r') as sql_f:
        sql_text = sql_f.read()

    if enable_optimizer_statistics:
        cursor.execute('SET yb_enable_optimizer_statistics=ON')
    explain_query = f"EXPLAIN {sql_text}"
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
        self.__sort_keys = kwargs.get('sort_keys')
        super(CustomIndentEncoder, self).__init__(**kwargs)

    def default(self, obj):
        return (self.FORMAT_SPEC.format(id(obj)) if isinstance(obj, NoIndent)
                else super(CustomIndentEncoder, self).default(obj))

    def encode(self, obj):
        format_spec = self.FORMAT_SPEC   # Local var to expedite access.
        json_repr = super(CustomIndentEncoder, self).encode(obj)  # Default JSON.

        # Replace any marked-up object ids in the JSON repr with the
        # value returned from the json.dumps() of the corresponding
        # wrapped Python object.
        for match in self.regex.finditer(json_repr):
            id = int(match.group(1))
            no_indent = PyObj_FromPtr(id)
            json_obj_repr = json.dumps(no_indent.value, sort_keys=self.__sort_keys)

            # Replace the matched id string with json formatted representation
            # of the corresponding Python object.
            json_repr = json_repr.replace(f'"{format_spec.format(id)}"', json_obj_repr)

        return json_repr


def export_statistics(cursor, relation_names, out_dir):
    statistics_file_name = f'{out_dir}/{STATISTICS_FILE_NAME}'
    print(f"Exporting data from pg_statistic and pg_class to file {statistics_file_name}")

    relation_names_filter = ""
    if relation_names:
        relation_names_str = ', '.join([f"\'{relation_name}\'" for relation_name in relation_names])

        relation_names_filter = f" and c.relname in ({relation_names_str}) "
    query = """
        SELECT row_to_json(t) FROM 
            (SELECT c.relname, c.reltuples, n.nspname
                FROM pg_class c JOIN pg_namespace n on c.relnamespace = n.oid and n.nspname = 'public' %s) t
        """ % relation_names_filter

    cursor.execute(query)
    rows_pg_class = cursor.fetchall()
    list_pg_class = [NoIndent(row[0]) for row in rows_pg_class]
    statistics_dict = {'pg_class': list_pg_class}
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
        """ % relation_names_filter

    cursor.execute(query)
    list_pg_statistic = []
    while True:
        if pg_statistic_rows := cursor.fetchmany(STATISTICS_FETCH_ROWS_SIZE):
            list_pg_statistic.extend(NoIndent(row[0]) for row in pg_statistic_rows)
        else:
            break
    statistics_dict['pg_statistic'] = list_pg_statistic
    statistics_json = json.dumps(statistics_dict, indent=4, cls=CustomIndentEncoder)

    with open(statistics_file_name, 'w') as statistics_file:
        statistics_file.write(statistics_json)


def export_ysql_pg_conf(cursor, out_dir):
    ysql_pg_conf_file_name = f'{out_dir}/{YSQL_PG_CONF_FILE_NAME}'
    print(f"Exporting ysql_pg_conf to {ysql_pg_conf_file_name}")
    ysql_pg_conf_csv = "";
    cursor.execute('SELECT name, setting, source from pg_settings where setting <> boot_val')
    while True:
        if pg_settings_rows := cursor.fetchmany(STATISTICS_FETCH_ROWS_SIZE):
            for row in pg_settings_rows:
                if row[0] in CBO_RELEVANT_GUC_PARAMS:
                    ysql_pg_conf_csv += f'{row[0]}={row[1]}, '
        else:
            break
    with open(ysql_pg_conf_file_name, 'w') as ysql_pg_conf_file:
        ysql_pg_conf_file.write(ysql_pg_conf_csv)


def export_gflags(host, out_dir):
    gflags_file_name = f'{out_dir}/{GFLAGS_FILE_NAME}'
    print(f"Exporting gflags to {gflags_file_name}")
    gflags_dict = {}
    try :
        with urllib.request.urlopen(f'http://{host}:7000/api/v1/varz') as url:
            data = json.load(url)
            for flag in data['flags']:
                if flag['type'] == 'Custom':
                    gflags_dict[flag['name']] = flag['value']
        
        gflags_json = json.dumps(gflags_dict, indent = 4)
        with open(gflags_file_name, 'w') as gflags_file:
            gflags_file.write(gflags_json)
    except Exception as e:
        print ("Failed to get gflags.")
        return


def set_extra_float_digits(cursor, digits):
    cursor.execute(f'SET extra_float_digits = {digits}')


def main():
    args = parse_cmd_line()
    connection_dict = get_connection_dict(args)
    conn, cursor = connect_database(connection_dict)
    Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    out_dir_abs_path = os.path.abspath(args.out_dir)
    relation_names = []
    if args.sql_file is not None:
        relation_names = get_relation_names_in_query(cursor, args.sql_file)
        export_query_file(args.sql_file, out_dir_abs_path)
        export_query_plan(cursor, args.sql_file, out_dir_abs_path, args.enable_optimizer_statistics)
    set_extra_float_digits(cursor, 3)
    extract_ddl(connection_dict, relation_names, out_dir_abs_path)
    export_statistics(cursor, relation_names, out_dir_abs_path)
    export_ysql_pg_conf(cursor, out_dir_abs_path)
    export_gflags(args.host, out_dir_abs_path)
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
