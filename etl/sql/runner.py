"""
SQL Runner class.

The SQL Runner is a part of the Python ETL. It allows to run arbitrary SQL scripts
from given files. Also it contains a number of various helper functions.
"""
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import psycopg2.extras

from etl import config
from etl import constants
from etl.common.db import get_redshift

__all__ = (
    'runner',
)


class SqlRunner:

    def read_query(self, path):
        try:
            with open(path) as sql_script:
                sql_query = sql_script.read()
                return sql_query
        except Exception as ex:
            self.error('Failed to read %s  because of %s', path, str(ex))
            raise

    def exec_query(self, cursor, sql_query, params=None, query_description='', show_error=True):
        """
        :param cursor: DB cursor
        :param sql_query: SQL query string
        :param params:  Query params for SQL templates
        :param query_description: Optional query description parameter used for logs only
        :param show_error: log a SQL error
        :return:
        """
        try:
            if params is not None:
                # Plain formatting is used because not only SQL params can be
                # substituted but also fields or table names etc
                sql_query = sql_query.format(**params)

            start_time = time.time()
            cursor.execute(sql_query)
            elapsed_time = time.time() - start_time

            affected_rows = cursor.rowcount if cursor.rowcount > 0 else 0
            message = 'Query: {} Finished in: {:.2f}s Affected rows: {}'.format(
                query_description, elapsed_time, affected_rows)

            query_info = {
                'description': query_description,
                'duration_secs': elapsed_time,
                'affected_rows': affected_rows,
            }

            process_info = {
                'process_id': os.getpid(),
                'parent_process_id': os.getppid(),
                'thread_id': threading.get_ident(),
                'process_name': os.path.basename(' '.join(sys.argv)),
            }
            self.info(message, query=query_info, process=process_info)

            return affected_rows
        except psycopg2.DatabaseError:
            if show_error:
                self.error('SQL runner error', exc_info=show_error)
            raise

    def make_sql_script(self, template_path, params):
        """
        Create a new SQL file based on a template. The new SQL files are stored outside the repo.
        """
        sql_query = self.read_query(template_path)
        sql_file = os.path.basename(template_path)
        new_sql_path = os.path.join('/tmp/', sql_file)
        self.debug('Created new SQL script %s', new_sql_path)

        with open(new_sql_path, 'w') as sql:
            sql.write(sql_query.format(**params))

        return new_sql_path

    def exec_sql_script(self, path, params=None, show_error=True, explicit_commit=False):
        """
        Execute a single SQL script
        """
        sql_query = self.read_query(path)
        with get_redshift() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                result = self.exec_query(cursor, sql_query, params, show_error=show_error, query_description=path)
                if explicit_commit:
                    conn.commit()
                return result

    def exec_sql_scripts(self, paths, show_error=True, explicit_commit=False):
        """
        Execute multiple SQL scripts one by one
        """
        affected_rows = 0
        with get_redshift() as conn:
            for path in paths:
                sql_query = self.read_query(path)
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    affected_rows += self.exec_query(cursor, sql_query, show_error=show_error, query_description=path)
            if explicit_commit:
                conn.commit()
        return affected_rows

    def exec_sql_scripts_parallel(self, script_paths):
        """
        Execute multiple SQL scripts in parallel using threads
        """
        with ThreadPoolExecutor(max_workers=config.ETL_THREAD_POOL_SIZE) as executor:
            return list(executor.map(self.exec_sql_script, script_paths))

    def exec_analyze_table(self, table_name, scheme='public'):
        """
        Perform the ANALYZE operation on a given table
        """
        if not isinstance(table_name, str):
            self.error('ERROR: Expected table name as a string received %s', table_name)
            return 0

        # Detect if scheme is present in a table name
        if '.' not in table_name:
            table_name = '"{}"."{}"'.format(scheme, table_name)

        analyze_query = 'analyze {}'.format(table_name)
        with get_redshift() as conn:
            with conn.cursor() as cursor:
                return self.exec_query(cursor, analyze_query, query_description='ANALYZE {}'.format(table_name))

    def exec_analyze_tables(self, table_names, scheme='public'):
        """
        Perform the ANALYZE operation on multiple tables
        """
        if not isinstance(table_names, list) and not isinstance(table_names, tuple):
            self.error(
                'ERROR: Expected table names as a list or tuple received %s', table_names)
            return 0

        analyze_query = 'analyze {}'
        affected_rows = 0
        with get_redshift() as conn:
            with conn.cursor() as cursor:
                for table_name in table_names:
                    # Detect if scheme is present in a table name
                    if '.' not in table_name:
                        table_name = '"{}"."{}"'.format(scheme, table_name)

                    affected_rows += self.exec_query(
                        cursor, analyze_query.format(table_name), query_description='ANALYZE {}'.format(table_name))
        return affected_rows

    def exec_analyze_tables_parallel(self, table_names):
        """
        Perform the ANALYZE operation on multiple tables in parallel using threads
        """
        with ThreadPoolExecutor(max_workers=config.ETL_THREAD_POOL_SIZE) as executor:
            return list(executor.map(self.exec_analyze_table, table_names))

    def stage_deputy_tips_count_check(self):
        """
        A helper function to check deputy count
        """
        self.debug('Execute stage_deputy_tips_count_check')
        check_query = 'select count(*) as count from public.deputy_stage_employee_timesheet'
        with get_redshift() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(check_query)
                result = cursor.fetchone()
                return result.get('count', 0) > 0

    def stage_orders_count_check(self):
        """
        A helper function to check orders count
        """
        self.debug('Execute stage_orders_count_check')
        check_query = 'select count(*) as count from xenial_pos_stage_order_details'
        with get_redshift() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(check_query)
                result = cursor.fetchone()
                return result.get('count', 0) > 0

    def stage_drawers_count_check(self):
        """
        A helper function to check drawers count
        """
        self.debug('Execute stage_orders_count_check')
        check_query = 'select count(*) as count from xenial_pos_stage_drawer_details'
        with get_redshift() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(check_query)
                result = cursor.fetchone()
                return result.get('count', 0) > 0

    def stage_deposits_count_check(self):
        """
        A helper function to check deposits count
        """
        self.debug('Execute stage_deposits_count_check')
        check_query = 'select count(*) as count from xenial_pos_stage_deposit_details'
        with get_redshift() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(check_query)
                result = cursor.fetchone()
                return result.get('count', 0) > 0

    def stage_payrolls_count_check(self):
        """
        A helper function to check payrolls count
        """
        self.debug('Execute stage_payrolls_count_check')
        check_query = 'select count(*) as count from xenial_boh_stage_payroll_details'
        with get_redshift() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(check_query)
                result = cursor.fetchone()
                return result.get('count', 0) > 0

    def wait_vacuum_finish(self):
        with get_redshift() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                while True:
                    cursor.execute("""
                    select
                      count(userid) as cnt
                    from
                      stv_recents
                    where
                      status='Running' and query ilike '%VACUUM%';
                    """)
                    if cursor.fetchone()['cnt'] == 0:
                        return

                    self.info('Wait for VACUUM operation to finish')
                    time.sleep(config.ETL_DB_TIMEOUT)

    def get_tables_list(self, scheme='public'):
        self.debug('Execute get_tables_list')
        with get_redshift() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute("""
                select
                  distinct("tablename") as tablename 
                from
                  pg_table_def
                where
                  "schemaname"=%s;
                """, (scheme, ))

                return [row['tablename'] for row in cursor.fetchall()]

    def table_exists(self, table_name, scheme='public'):
        self.debug('Execute table_exists')
        with get_redshift() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute("""
                select
                  distinct("tablename") as tablename 
                from
                  pg_table_def
                where
                  "tablename"=%s and
                  "schemaname"=%s;
                """, (table_name, scheme))

                return len(cursor.fetchall()) > 0

    def exec_vacuum_db(self, vacuum_option='FULL'):
        """
        Perform the VACUUM operation on a given DB

        From Redshift documentation:
        If you don't specify a table name, the vacuum operation applies to all tables in the current database.
        """
        vacuum_query = 'END TRANSACTION; VACUUM {vacuum_option}'.format(vacuum_option=vacuum_option)
        with get_redshift() as conn:
            with conn.cursor() as cursor:
                return self.exec_query(cursor, vacuum_query, query_description='VACUUM')

    def exec_vacuum_table(self, table_name, scheme='public', vacuum_option='FULL'):
        """
        Perform the VACUUM operation on a give table
        """
        # NOTE: The threshold parameter isn't supported at the moment
        if vacuum_option.upper() not in constants.VACUUM_PARAMS:
            self.error('ERROR: Unknown vacuum param %s', vacuum_option)
            return 0

        if not isinstance(table_name, str):
            self.error('ERROR: Expected table name as a string received %s', table_name)
            return 0

        # Detect if scheme is present in table a name
        if '.' not in table_name:
            table_name = '"{}"."{}"'.format(scheme, table_name)

        vacuum_query = 'END TRANSACTION; VACUUM {vacuum_option} {table_name}'.format(
            vacuum_option=vacuum_option,
            table_name=table_name)
        with get_redshift() as conn:
            with conn.cursor() as cursor:
                return self.exec_query(cursor, vacuum_query, query_description='VACUUM {}'.format(table_name))

    def exec_vacuum_tables(self, table_names, scheme='public', vacuum_option='FULL'):
        """
        Perform the VACUUM operation on multiple tables
        """
        # NOTE: The threshold parameter isn't supported at the moment
        if vacuum_option.upper() not in constants.VACUUM_PARAMS:
            self.error('ERROR: Unknown vacuum param %s', vacuum_option)
            return 0

        if not isinstance(table_names, list) and not isinstance(table_names, tuple):
            self.error('ERROR: Expected table names as a list or tuple received %s', table_names)
            return 0

        vacuum_query = 'END TRANSACTION; VACUUM {vacuum_option} {table_name}'
        affected_rows = 0
        with get_redshift() as conn:
            with conn.cursor() as cursor:
                for table_name in table_names:
                    # Detect if scheme is present in table a name
                    if '.' not in table_name:
                        table_name = '"{}"."{}"'.format(scheme, table_name)

                    affected_rows += self.exec_query(cursor, vacuum_query.format(
                        vacuum_option=vacuum_option,
                        table_name=table_name,
                    ), query_description='VACUUM {} {}'.format(vacuum_option, table_name))
        return affected_rows

    def exec_truncate_table(self, table_name, scheme='public'):
        """
        Perform the TRUNCATE operation on a give table
        """
        if not isinstance(table_name, str):
            self.error('ERROR: Expected table name as a string received %s', table_name)
            return 0

        # Detect if scheme is present in a table name
        if '.' not in table_name:
            table_name = '"{}"."{}"'.format(scheme, table_name)

        truncate_query = 'truncate {}'.format(table_name)
        with get_redshift() as conn:
            with conn.cursor() as cursor:
                return self.exec_query(cursor, truncate_query, query_description='TRUNCATE {}'.format(table_name))

    def exec_truncate_tables(self, table_names, scheme='public'):
        """
        Perform the TRUNCATE operation on multiple tables
        """
        if not isinstance(table_names, list) and not isinstance(table_names, tuple):
            self.error('ERROR: Expected table names as a list or tuple received %s', table_names)
            return 0

        truncate_query = 'truncate {}'
        affected_rows = 0
        with get_redshift() as conn:
            with conn.cursor() as cursor:
                for table_name in table_names:
                    # Detect if scheme is present in a table name
                    if '.' not in table_name:
                        table_name = '"{}"."{}"'.format(scheme, table_name)

                    affected_rows += self.exec_query(
                        cursor, truncate_query.format(table_name), query_description='TRUNCATE {}'.format(table_name))
        return affected_rows


runner = SqlRunner()