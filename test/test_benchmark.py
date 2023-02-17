#!/usr/bin/env python3

'''
Objective :
 * Connect to a target database with TPCDS dataset. See below for steps to create such database.
 * For each query in tpcds_queries folder 
    * export DDL, query plan and statistics from the target database.
    * Create a test database and run the DDL file, import the statistics and get the query plan.
    * Compare the query plan and fail if these don't match.
'''

import argparse
import psycopg2
import os, subprocess
from pathlib import Path
import difflib
import shutil
import sys
import difflib


def get_project_root() -> Path:
    return Path(__file__).parent.parent

PROJECT_DIR = str(get_project_root())
TEST_OUTDIR = PROJECT_DIR + '/test_out_dir'
EXPORT_SCRIPT = 'export_query_planner_data.py'
IMPORT_SCRIPT = 'import_query_planner_stats.py'

def parse_arguments():
    parser = argparse.ArgumentParser('test_with_benchmark', 
            description='Test the framework to reproduce query plans on benchmarks')
    parser.add_argument('-b', '--benchmark', required=True, help='Name of the benchmark')
    parser.add_argument('--target_host', help='Hostname or IP address, default localhost', default='localhost')
    parser.add_argument('--target_port', help='Port number, default 5433', default=5433)
    parser.add_argument('--target_user', help='YugabyteDB username, default yugabyte', default='yugabyte')
    parser.add_argument('--target_password', help='Password, default no password')
    parser.add_argument('--target_database', help='Target Database name, default benchmark name with "_db" suffix')
    parser.add_argument('--test_host', help='Hostname or IP address, default same as TARGET_HOST')
    parser.add_argument('--test_port', help='Port number, , default same as TARGET_PORT')
    parser.add_argument('--test_user', help='YugabyteDB username, default same as TARGET_USER')
    parser.add_argument('--test_password', help='Password, default same as TARGET_PASSWORD')
    parser.add_argument('--ignore_ran_tests', action='store_true', help='Ignore tests for which an outdir exists')
    parser.add_argument('--enable_optimizer_statistics', action='store_true', help='Set yb_enable_optimizer_statistics=ON before running explain on query')
    parser.add_argument('--colocation', action='store_true', help='Creates test database with colocation ie. all tables are colocated on a single tablet')

    args = parser.parse_args()

    if args.target_database is None:
        args.target_database = args.benchmark + '_db'
    if args.test_host is None:
        args.test_host = args.target_host
    if args.test_port is None:
        args.test_port = args.target_port
    if args.test_user is None:
        args.test_user = args.target_user
    if args.test_password is None:
        args.test_password = args.target_password

    return args

def connect_test_database(args, test_db_name):
    connectionDict = {
        'host': args.test_host,
        'port': args.test_port,
        'user': args.test_user,
        'password': args.test_password,
        'database': test_db_name
    }
    test_conn = psycopg2.connect(**connectionDict)
    test_cursor = test_conn.cursor()
    return test_conn, test_cursor

def export_query_plan(args, test_db_name, query_file, outdir):
    query_plan_file_name = outdir + '/sim_query_plan.txt'
    print("Exporting query plan to %s" % query_plan_file_name)
    with open(query_file, 'r') as sql_f:
        sql_text = sql_f.read()

    explain_query = "EXPLAIN %s" % sql_text
    test_conn, test_cursor = connect_test_database(args, test_db_name)
    if args.enable_optimizer_statistics:
        test_cursor.execute('SET yb_enable_optimizer_statistics=ON')
    test_cursor.execute(explain_query)    
    query_plan = test_cursor.fetchall()
    with open(query_plan_file_name, 'w') as query_plan_file:
        for tuple in query_plan:
            query_plan_file.write(tuple[0] + '\n')

    test_cursor.close()
    test_conn.close()

def get_test_connection_str(args):
    connection_str = ['-h', args.test_host, '-p', str(args.test_port), '-U', args.test_user]
    return connection_str

'''
Create a new database for testing query on the test instance
'''
def create_test_database(args, test_db_name):
    connection_str = get_test_connection_str(args)
    
    assert_binary_in_path('createdb')

    cmd = ['createdb']
    cmd.extend(connection_str)
    cmd.append(test_db_name)
    if args.colocation:
        cmd.append('--colocation')

    my_env = os.environ.copy()
    if args.test_password is not None:
        my_env["PGPASSWORD"] = args.test_password
    result = subprocess.run(cmd, env=my_env)
    if result.stderr is not None:
        sys.stderr.writelines('Error in createdb: ' + result.stderr)
        sys.exit(1)
    else:
        print ('Created database ' + test_db_name)

def assert_binary_in_path(binary_name):
    if not shutil.which(binary_name):
        sys.stderr.writelines(binary_name + ' binary not found.\n\n')
        sys.exit(1)

'''
Drop the database on test instance
'''
def drop_test_database(args, test_db_name):
    connection_str = get_test_connection_str(args)
    
    assert_binary_in_path('dropdb')

    cmd = ['dropdb']
    cmd.extend(connection_str)
    cmd.append(test_db_name)

    my_env = os.environ.copy()
    if args.test_password is not None:
        my_env["PGPASSWORD"] = args.test_password
    result = subprocess.run(cmd, env=my_env)
    if result.stderr is not None:
        sys.stderr.writelines('Error in dropdb: ' + result.stderr)
        sys.exit(1)
    else:
        print ('Dropped database ' + test_db_name)

def run_export_script(args, query_outdir, query_file_name_abs):
    cmd = ['python', PROJECT_DIR + '/' + EXPORT_SCRIPT, 
            '-H', args.target_host,
            '-P', str(args.target_port),
            '-D', args.target_database,
            '-u', args.target_user,
            '-o', query_outdir,
            '-q', query_file_name_abs]
            
    if args.target_password is not None:
        cmd.extend(['-p', args.target_password])
    if args.enable_optimizer_statistics:
        cmd.append('--enable_optimizer_statistics')
    
    subprocess.run(cmd)

def run_ddl_on_test_database(args, test_db_name, ddl_file):
    assert_binary_in_path('ysqlsh')

    connection_str = get_test_connection_str(args)

    cmd = ['ysqlsh']
    cmd.extend(connection_str)
    cmd.extend(['-d', test_db_name])
    cmd.extend(['-f', ddl_file])

    my_env = os.environ.copy()
    if args.test_password is not None:
        my_env["PGPASSWORD"] = args.test_password
    subprocess.run(cmd, env=my_env)

def run_import_script(args, test_db_name, statistics_file_name):
    cmd = ['python', PROJECT_DIR + '/' + IMPORT_SCRIPT, 
            "-H", args.test_host,
            "-P", str(args.test_port),
            "-D", test_db_name,
            "-u", args.test_user,
            "-s", statistics_file_name]
    if args.target_password is not None:
        cmd.extend(['-p', args.target_password])
    
    subprocess.run(cmd)

def diff_query_plans(outdir):
    with open(outdir + '/query_plan.txt') as target_query_plan:
        target_query_plan_text = target_query_plan.readlines()

    with open(outdir + '/sim_query_plan.txt') as sim_query_plan:
        sim_query_plan_text = sim_query_plan.readlines()

    query_plan_diff = difflib.unified_diff(target_query_plan_text, sim_query_plan_text, fromfile=outdir + '/query_plan.txt', tofile=outdir + '/sim_query_plan.txt')
    if query_plan_diff is not None:
        print('Test fail')
        with open(outdir + '/query_plan_diff.txt', 'w') as query_plan_diff_file:
            for line in query_plan_diff:
                query_plan_diff_file.write(line)

def main():
    args = parse_arguments()
    benchmark_queries_path = PROJECT_DIR + '/test/' + args.benchmark + '_queries'
    if not os.path.isdir(benchmark_queries_path):
        sys.stderr.write('Test queries path not found : ' + benchmark_queries_path)
        sys.exit(1)
    print ('Testing queries in ' + benchmark_queries_path)
    list_query_files = os.listdir(benchmark_queries_path)
    list_query_files.sort()
    for query_file_name in list_query_files:
        query_name = Path(query_file_name).stem
        query_file_name_abs = os.path.join(benchmark_queries_path, query_file_name)
        if os.path.isfile(query_file_name_abs):
            query_outdir = TEST_OUTDIR + '/' + args.benchmark + '/' + query_name
            if args.ignore_ran_tests and os.path.exists(query_outdir):
                print ('Ignoring previously ran test ' + args.benchmark + ' : ' + query_name)
                continue
            print ('Testing ' + query_name)
            run_export_script(args, query_outdir, query_file_name_abs)
            test_db_name = args.benchmark + '_' + query_name + '_test_db'
            create_test_database(args, test_db_name)
            run_ddl_on_test_database(args, test_db_name, query_outdir + '/ddl.sql')
            run_import_script(args, test_db_name, query_outdir + '/statistics.json')
            export_query_plan(args, test_db_name, query_file_name_abs, query_outdir)
            diff_query_plans(query_outdir)
            drop_test_database(args, test_db_name)

if __name__ == "__main__":
    main()