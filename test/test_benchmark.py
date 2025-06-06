#!/usr/bin/env python3

'''
Objective :
 * Connect to a prod database with benchmark dataset. See below for steps to create such database.
 * For each query in <becnhmark>_queries folder
    * export DDL, query plan and statistics from the prod database.
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
import logging
import re

logger = logging.getLogger(__name__)

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

def parse_arguments():
    parser = argparse.ArgumentParser('test_with_benchmark',
            description='Test the framework to reproduce query plans on benchmarks')
    parser.add_argument('-d', '--debug', action='store_true', help='Set log level to DEBUG')
    parser.add_argument('-b', '--benchmark', required=True, help='Name of the benchmark')
    parser.add_argument('--yb_mode', action='store_true', help='Use this option if prod database is YugabyteDB')
    parser.add_argument('--benchmark_path', help='Path to benchmark files')
    parser.add_argument('--create_prod_db', action='store_true', help='')
    parser.add_argument('--prod_host', help='Hostname or IP address of production server, default localhost', default='localhost')
    parser.add_argument('--prod_port', help='Port number of production server, default 5432')
    parser.add_argument('--prod_user', help='Username of production server')
    parser.add_argument('--prod_password', help='Password of production server, default no password')
    parser.add_argument('--prod_database', help='Database name in production server, default benchmark name with "_db" suffix')
    parser.add_argument('--test_host', help='Hostname or IP address, default same as TARGET_HOST')
    parser.add_argument('--test_port', help='Port number, , default same as TARGET_PORT')
    parser.add_argument('--test_user', help='YugabyteDB username, default same as TARGET_USER')
    parser.add_argument('--test_password', help='Password, default same as TARGET_PASSWORD')
    parser.add_argument('--ignore_ran_tests', action='store_true', help='Ignore tests for which an outdir exists')
    parser.add_argument('--enable_base_scans_cost_model', action='store_true', help='Set yb_enable_base_scans_cost_model=ON before running explain on query')
    parser.add_argument('--colocation', action='store_true', help='Creates test database with colocation ie. all tables are colocated on a single tablet')
    parser.add_argument('--outdir', help='Test output directory')

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        
    if not args.benchmark_path:
        args.benchmark_path = PROJECT_DIR + '/test/' + args.benchmark

    if args.yb_mode:
        if args.prod_user is None:
            args.prod_user = 'yugabyte'
        if args.prod_port is None:
            args.prod_port = 5433
    else:
        if args.prod_user is None:
            args.prod_user = 'gaurav'
        if args.prod_port is None:
            args.prod_port = 5432

    if args.test_host is None:
        args.test_host = args.prod_host
    if args.test_port is None:
        args.test_port = args.prod_port
    if args.test_user is None:
        args.test_user = args.prod_user
    if args.test_password is None:
        args.test_password = args.prod_password

    if args.prod_database is None:
        args.prod_database = args.benchmark + '_db'

    if args.colocation and not args.yb_mode:
        logger.error(f'--colocation can only be used with --yb_mode')
        logger.error(f'Colocation is a YugabyteDB feature and not supported on Postgres')
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

def export_query_plan(args, test_db_name, overridden_gucs_file, query_file, outdir):
    query_plan_file_name = outdir + '/sim_query_plan.txt'
    logger.debug("Exporting query plan to %s" % query_plan_file_name)
    sql_text = ""
    if os.path.exists(overridden_gucs_file):
        with open(overridden_gucs_file, 'r') as gucs_f:
            sql_text += gucs_f.read()    
    with open(query_file, 'r') as sql_f:
        sql_text += 'EXPLAIN ' + sql_f.read()

    test_conn, test_cursor = connect_test_database(args, test_db_name)
    if args.enable_base_scans_cost_model and args.yb_mode:
        test_cursor.execute('SET yb_enable_base_scans_cost_model=ON')
    if not args.yb_mode:
        test_cursor.execute('SET enable_cbo_statistics_simulation=ON')
    test_cursor.execute(sql_text)
    query_plan = test_cursor.fetchall()
    with open(query_plan_file_name, 'w') as query_plan_file:
        for tuple in query_plan:
            query_plan_file.write(tuple[0] + '\n')

    test_cursor.close()
    test_conn.close()

def get_prod_connection_str(args):
    connection_str = ['-h', args.prod_host, '-p', str(args.prod_port), '-U', args.prod_user]
    return connection_str

def get_test_connection_str(args):
    connection_str = ['-h', args.test_host, '-p', str(args.test_port), '-U', args.test_user]
    return connection_str

def assert_binary_in_path(binary_name):
    if not shutil.which(binary_name):
        logger.error(binary_name + ' binary not found.')
        sys.exit(1)

def run_cbo_stat_dump(args, query_outdir, query_file_name_abs):
    cmd = ['python3.13', PROJECT_DIR + '/' + EXPORT_SCRIPT,
            '-h', args.prod_host,
            '-p', str(args.prod_port),
            '-d', args.prod_database,
            '-u', args.prod_user,
            '-o', query_outdir,
            '-q', query_file_name_abs]

    if args.prod_password is not None:
        cmd.extend(['-W', args.prod_password])
    if args.enable_base_scans_cost_model and args.yb_mode:
        cmd.append('--enable_base_scans_cost_model')
    if args.yb_mode:
        cmd.append('--yb_mode')

    subprocess.run(cmd)

def run_sql_on_test_database(args, test_db_name, sql_file):
    SH_BIN = get_sh_bin(args)
    assert_binary_in_path(SH_BIN)

    connection_str = get_test_connection_str(args)

    cmd = [SH_BIN]
    cmd.extend(connection_str)
    cmd.extend(['-q'])
    cmd.extend(['-d', test_db_name])
    cmd.extend(['-f', sql_file])

    my_env = os.environ.copy()
    if args.test_password is not None:
        my_env["PGPASSWORD"] = args.test_password
    subprocess.run(cmd, env=my_env)

def query_plans_match(outdir):
    with open(outdir + '/query_plan.txt') as prod_query_plan:
        prod_query_plan_text = prod_query_plan.readlines()

    with open(outdir + '/sim_query_plan.txt') as sim_query_plan:
        sim_query_plan_text = sim_query_plan.readlines()

    query_plan_diff = list(difflib.unified_diff(prod_query_plan_text, sim_query_plan_text, fromfile=outdir + '/query_plan.txt', tofile=outdir + '/sim_query_plan.txt'))
    if query_plan_diff:
        logger.error('Test fail')
        with open(outdir + '/query_plan_diff.txt', 'w') as query_plan_diff_file:
            for line in query_plan_diff:
                query_plan_diff_file.write(line)
        return False
    return True

def get_connection_str(host: Host, args):
    if host == Host.TARGET:
        return get_prod_connection_str(args)
    elif host == Host.TEST:
        return get_test_connection_str(args)

'''
Drop the database on test instance
'''
def drop_database(host: Host, args, test_db_name):
    connection_str = get_connection_str(host, args)

    assert_binary_in_path('dropdb')

    cmd = ['dropdb']
    cmd.extend(['--if-exists'])
    cmd.extend(connection_str)
    cmd.append(test_db_name)

    my_env = os.environ.copy()
    if args.test_password is not None:
        my_env["PGPASSWORD"] = args.test_password
    try:
        result = subprocess.run(cmd, env=my_env, capture_output=True, check=True)
    except subprocess.CalledProcessError as e:
        logger.error('Error in dropdb: ' + str(e))
        sys.exit(1)
    else:
        logger.debug ('Dropped database ' + test_db_name)

def create_database(host: Host, args, benchmark_name):
    prod_connection_str = get_connection_str(host, args)

    assert_binary_in_path('createdb')

    cmd = ['createdb']
    cmd.extend(prod_connection_str)
    cmd.append(benchmark_name)
    if args.colocation:
        cmd.append('--colocation')

    my_env = os.environ.copy()
    if args.test_password is not None:
        my_env["PGPASSWORD"] = args.test_password
    try:
        subprocess.run(cmd, env=my_env)
    except subprocess.CalledProcessError as e:
        logger.error('Error in createdb: ' + str(e))
        sys.exit(1)
    else:
        logger.debug ('Created database ' + benchmark_name)

def get_sh_bin(args):
    SH_BIN = PSQL_BIN
    if args.yb_mode:
        SH_BIN = YSQLSH_BIN
    return SH_BIN

def execute_create_sql(host, args, benchmark_name, benchmark_create_path):
    assert(host == Host.TARGET)
    prod_connection_str = get_connection_str(host, args)

    SH_BIN = get_sh_bin(args)
    assert_binary_in_path(SH_BIN)

    cmd = [SH_BIN]
    cmd.extend(prod_connection_str)
    cmd.extend(['-q'])
    cmd.extend(['-d', benchmark_name])
    cmd.extend(['-f', benchmark_create_path])

    my_env = os.environ.copy()
    if args.test_password is not None:
        my_env["PGPASSWORD"] = args.test_password
    try:
        subprocess.run(cmd, env=my_env)
    except subprocess.CalledProcessError as e:
        logger.error('Error in creating prod database: ' + str(e))
        sys.exit(1)
    else:
        logger.debug ('Executed ' + benchmark_create_path)

def main():
    args = parse_arguments()
    if not os.path.isdir(args.benchmark_path):
        sys.stderr.write('Benchmark path not found : ' + args.benchmark_path)
        sys.exit(1)

    if args.yb_mode:
        benchmark_create_path = args.benchmark_path + '/create.yb.sql'
    else:
        benchmark_create_path = args.benchmark_path + '/create.sql'

    if args.create_prod_db:
        if not os.path.exists(benchmark_create_path):
            logger.fatal(f'{benchmark_create_path} is needed to create production database.')
            exit(1)

        logger.info(f'Creating production database using {benchmark_create_path}')
        logger.debug(f'Dropping "{args.prod_database}" database on the prod.')
        drop_database(Host.TARGET, args, args.prod_database)
        logger.debug(f'Creating "{args.prod_database}" database on the prod.')
        create_database(Host.TARGET, args, args.prod_database)
        logger.debug(f'Executing "{benchmark_create_path}" on the prod.')
        execute_create_sql(Host.TARGET, args, args.prod_database, benchmark_create_path)

    benchmark_queries_path = args.benchmark_path + '/queries'
    if not os.path.isdir(benchmark_queries_path):
        logger.fatal('Test queries path not found : ' + benchmark_queries_path)
        sys.exit(1)

    logger.info('Testing queries in ' + benchmark_queries_path)
    list_query_files = os.listdir(benchmark_queries_path)
    list_query_files.sort()
    failed_queries = []
    for query_file_name in list_query_files:
        query_name = Path(query_file_name).stem
        query_file_name_abs = os.path.join(benchmark_queries_path, query_file_name)
        if os.path.isfile(query_file_name_abs):
            query_outdir = args.outdir + '/' + query_name
            if args.ignore_ran_tests and os.path.exists(query_outdir):
                logger.debug ('Ignoring previously ran test ' + args.benchmark + ' : ' + query_name)
                continue
            logger.info ('Testing ' + query_name)
            run_cbo_stat_dump(args, query_outdir, query_file_name_abs)
            test_db_name = args.benchmark + '_' + query_name + '_test_db'
            drop_database(Host.TEST, args, test_db_name)
            create_database(Host.TEST, args, test_db_name)
            run_sql_on_test_database(args, test_db_name, query_outdir + '/ddl.sql')
            run_sql_on_test_database(args, test_db_name, query_outdir + '/import_statistics.sql')
            sleep(0.1)
            export_query_plan(args, test_db_name, query_outdir + '/overridden_gucs.sql', query_file_name_abs, query_outdir)
            if not query_plans_match(query_outdir):
                failed_queries.append([query_file_name, query_outdir + '/query_plan_diff.txt'])
            drop_database(Host.TEST, args, test_db_name)
    if failed_queries:
        logger.error("Following tests failed!")
        for query in failed_queries:
            logger.error(query[0] + ' : ' + query[1])
    else:
        logger.info("All tests passed!")

if __name__ == "__main__":
    main()