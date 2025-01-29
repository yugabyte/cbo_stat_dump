#!/usr/bin/env python3

'''
Objective :
 * Connect to a target database with benchmark dataset. See below for steps to create such database.
 * For each query in <becnhmark>_queries folder
    * export DDL, query plan and statistics from the target database.
    * Create a test database and run the DDL file, import the statistics and get the query plan.
    * Compare the query plan and create a diff if plans don't match.
'''

import argparse
import psycopg2
import os, subprocess
from pathlib import Path
import difflib
import shutil
import sys
import difflib
from enum import Enum
from time import sleep

class Host(Enum):
    TARGET = 1
    TEST = 2


def get_project_root() -> Path:
    return Path(__file__).parent.parent

PSQL_BIN = 'psql'
YSQLSH_BIN = 'ysqlsh'

PROJECT_DIR = str(get_project_root())
TEST_OUTDIR = PROJECT_DIR + '/test_out_dir'
EXPORT_SCRIPT = 'cbo_stat_dump'
IMPORT_SCRIPT = 'cbo_stat_load'

def parse_arguments():
    parser = argparse.ArgumentParser('test_with_benchmark',
            description='Test the framework to reproduce query plans on benchmarks')
    parser.add_argument('-b', '--benchmark', required=True, help='Name of the benchmark')
    parser.add_argument('--skip_create_db', action='store_true')
    parser.add_argument('--target_host', help='Hostname or IP address, default localhost', default='localhost')
    parser.add_argument('--target_port', help='Port number, default 5432')
    parser.add_argument('--target_user', help='YugabyteDB username, default yugabyte')
    parser.add_argument('--target_password', help='Password, default no password')
    parser.add_argument('--target_database', help='Target Database name, default benchmark name with "_db" suffix')
    parser.add_argument('--yb_mode', action='store_true', help='Use this option if target database is YugabyteDB')
    parser.add_argument('--test_host', help='Hostname or IP address, default same as TARGET_HOST')
    parser.add_argument('--test_port', help='Port number, , default same as TARGET_PORT')
    parser.add_argument('--test_user', help='YugabyteDB username, default same as TARGET_USER')
    parser.add_argument('--test_password', help='Password, default same as TARGET_PASSWORD')
    parser.add_argument('--ignore_ran_tests', action='store_true', help='Ignore tests for which an outdir exists')
    parser.add_argument('--enable_optimizer_statistics', action='store_true', help='Set yb_enable_optimizer_statistics=ON before running explain on query')
    parser.add_argument('--colocation', action='store_true', help='Creates test database with colocation ie. all tables are colocated on a single tablet')
    parser.add_argument('--outdir', help='Test output directory')

    args = parser.parse_args()

    if args.yb_mode:
        if args.target_user is None:
            args.target_user = 'yugabyte'
        if args.target_port is None:
            args.target_port = 5433
    else:
        if args.target_user is None:
            args.target_user = 'gaurav'
        if args.target_port is None:
            args.target_port = 5432

    if args.test_host is None:
        args.test_host = args.target_host
    if args.test_port is None:
        args.test_port = args.target_port
    if args.test_user is None:
        args.test_user = args.target_user
    if args.test_password is None:
        args.test_password = args.target_password

    if args.target_database is None:
        args.target_database = args.benchmark + '_db'

    if args.colocation and not args.yb_mode:
        sys.stderr.writelines(f'--colocation can only be used with --yb_mode\n')
        sys.stderr.writelines(f'Colocation is a YugabyteDB feature and not supported on Postgres\n')
        sys.exit(1)

    if args.outdir is None:
        args.outdir = TEST_OUTDIR + '/' + args.benchmark

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
    if args.enable_optimizer_statistics and args.yb_mode:
        test_cursor.execute('SET yb_enable_optimizer_statistics=ON')
    if not args.yb_mode:
        test_cursor.execute('SET enable_cbo_statistics_simulation=ON')
    test_cursor.execute(explain_query)
    query_plan = test_cursor.fetchall()
    with open(query_plan_file_name, 'w') as query_plan_file:
        for tuple in query_plan:
            query_plan_file.write(tuple[0] + '\n')

    test_cursor.close()
    test_conn.close()

def get_target_connection_str(args):
    connection_str = ['-h', args.target_host, '-p', str(args.target_port), '-U', args.target_user]
    return connection_str

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

def run_cbo_stat_dump(args, query_outdir, query_file_name_abs):
    cmd = ['python3.13', PROJECT_DIR + '/' + EXPORT_SCRIPT,
            '-h', args.target_host,
            '-p', str(args.target_port),
            '-d', args.target_database,
            '-u', args.target_user,
            '-o', query_outdir,
            '-q', query_file_name_abs]

    if args.target_password is not None:
        cmd.extend(['-W', args.target_password])
    if args.enable_optimizer_statistics and args.yb_mode:
        cmd.append('--enable_optimizer_statistics')
    if args.yb_mode:
        cmd.append('--yb_mode')

    subprocess.run(cmd)

def run_ddl_on_test_database(args, test_db_name, ddl_file):
    SH_BIN = PSQL_BIN
    if args.yb_mode:
        SH_BIN = YSQLSH_BIN
    assert_binary_in_path(SH_BIN)

    connection_str = get_test_connection_str(args)

    cmd = [SH_BIN]
    cmd.extend(connection_str)
    cmd.extend(['-d', test_db_name])
    cmd.extend(['-f', ddl_file])

    my_env = os.environ.copy()
    if args.test_password is not None:
        my_env["PGPASSWORD"] = args.test_password
    subprocess.run(cmd, env=my_env)

def run_cbo_stat_load(args, test_db_name, statistics_file_name):
    sys.stdout.write(f'Running {IMPORT_SCRIPT}\n')
    cmd = ['python3.13', PROJECT_DIR + '/' + IMPORT_SCRIPT,
            "-h", args.test_host,
            "-p", str(args.test_port),
            "-d", test_db_name,
            "-u", args.test_user,
            "-s", statistics_file_name]
    if args.test_password is not None:
        cmd.extend(['-W', args.test_password])
    if args.yb_mode:
        cmd.append('--yb_mode')

    subprocess.run(cmd)

def diff_query_plans(outdir):
    with open(outdir + '/query_plan.txt') as target_query_plan:
        target_query_plan_text = target_query_plan.readlines()

    with open(outdir + '/sim_query_plan.txt') as sim_query_plan:
        sim_query_plan_text = sim_query_plan.readlines()

    query_plan_diff = list(difflib.unified_diff(target_query_plan_text, sim_query_plan_text, fromfile=outdir + '/query_plan.txt', tofile=outdir + '/sim_query_plan.txt'))
    if query_plan_diff:
        print('Test fail')
        with open(outdir + '/query_plan_diff.txt', 'w') as query_plan_diff_file:
            for line in query_plan_diff:
                query_plan_diff_file.write(line)

def get_connection_str(host: Host, args):
    if host == Host.TARGET:
        return get_target_connection_str(args)
    elif host == Host.TEST:
        return get_test_connection_str(args)

'''
Drop the database on test instance
'''
def drop_database(host: Host, args, test_db_name):
    connection_str = get_connection_str(host, args)

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

def create_database(host: Host, args, benchmark_name):
    target_connection_str = get_connection_str(host, args)

    assert_binary_in_path('createdb')

    cmd = ['createdb']
    cmd.extend(target_connection_str)
    cmd.append(benchmark_name)
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
        print ('Created database ' + benchmark_name)

def get_sh_bin(args):
    SH_BIN = PSQL_BIN
    if args.yb_mode:
        SH_BIN = YSQLSH_BIN
    return SH_BIN

def execute_create_sql(host, args, benchmark_name, benchmark_create_path):
    assert(host == Host.TARGET)
    target_connection_str = get_connection_str(host, args)

    SH_BIN = get_sh_bin(args)
    assert_binary_in_path(SH_BIN)

    cmd = [SH_BIN]
    cmd.extend(target_connection_str)
    cmd.extend(['-d', benchmark_name])
    cmd.extend(['-f', benchmark_create_path])

    my_env = os.environ.copy()
    if args.test_password is not None:
        my_env["PGPASSWORD"] = args.test_password
    result = subprocess.run(cmd, env=my_env)
    if result.stderr is not None:
        sys.stderr.writelines('Error in creating target database: ' + result.stderr)
        sys.exit(1)
    else:
        print ('Executed ' + benchmark_create_path)

def main():
    args = parse_arguments()
    benchmark_path = PROJECT_DIR + '/test/' + args.benchmark
    if not os.path.isdir(benchmark_path):
        sys.stderr.write('Benchmark path not found : ' + benchmark_path)
        sys.exit(1)

    if not args.yb_mode:
        benchmark_create_path = benchmark_path + '/create.yb.sql'
    else:
        benchmark_create_path = benchmark_path + '/create.sql'

    if os.path.exists(benchmark_create_path):
        sys.stdout.write(f'Dropping "{args.target_database}" database on the target.\n')
        drop_database(Host.TARGET, args, args.target_database)
        sys.stdout.write(f'Creating "{args.target_database}" database on the target.\n')
        create_database(Host.TARGET, args, args.target_database)
        sys.stdout.write(f'Executing "{benchmark_create_path}" on the target.\n')
        execute_create_sql(Host.TARGET, args, args.target_database, benchmark_create_path)

    benchmark_queries_path = PROJECT_DIR + '/test/' + args.benchmark + '/queries'
    if not os.path.isdir(benchmark_queries_path):
        sys.stderr.write('Test queries path not found : ' + benchmark_queries_path + '\n')
        sys.exit(1)

    sys.stdout.write('Testing queries in ' + benchmark_queries_path + '\n')
    list_query_files = os.listdir(benchmark_queries_path)
    list_query_files.sort()
    for query_file_name in list_query_files:
        query_name = Path(query_file_name).stem
        query_file_name_abs = os.path.join(benchmark_queries_path, query_file_name)
        if os.path.isfile(query_file_name_abs):
            query_outdir = args.outdir + '/' + query_name
            if args.ignore_ran_tests and os.path.exists(query_outdir):
                print ('Ignoring previously ran test ' + args.benchmark + ' : ' + query_name)
                continue
            print ('Testing ' + query_name)
            run_cbo_stat_dump(args, query_outdir, query_file_name_abs)
            test_db_name = args.benchmark + '_' + query_name + '_test_db'
            drop_database(Host.TEST, args, test_db_name)
            create_database(Host.TEST, args, test_db_name)
            run_ddl_on_test_database(args, test_db_name, query_outdir + '/ddl.sql')
            run_cbo_stat_load(args, test_db_name, query_outdir + '/statistics.json')
            sleep(0.1)
            export_query_plan(args, test_db_name, query_file_name_abs, query_outdir)
            diff_query_plans(query_outdir)
            # drop_test_database(args, test_db_name)

if __name__ == "__main__":
    main()