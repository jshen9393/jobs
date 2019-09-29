"""
SQL Runner class.

The SQL Runner is a part of the Python ETL. It allows to run arbitrary SQL scripts
from given files. Also it contains a number of various helper functions.
"""
import os
import sys
import time
import threading

import psycopg2
import psycopg2.extras

from etl.common.db import get_postgres

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

            return affected_rows
        except psycopg2.DatabaseError as error:
            raise error

    def make_sql_script(self, template_path, params):
        """
        Create a new SQL file based on a template. The new SQL files are stored outside the repo.
        """
        sql_query = self.read_query(template_path)
        sql_file = os.path.basename(template_path)
        new_sql_path = os.path.join('/tmp/', sql_file)

        with open(new_sql_path, 'w') as sql:
            sql.write(sql_query.format(**params))

        return new_sql_path

    def exec_sql_script(self, path, params=None, show_error=True, explicit_commit=False):
        """
        Execute a single SQL script
        """
        sql_query = self.read_query(path)
        with get_postgres() as conn:
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
        with get_postgres() as conn:
            for path in paths:
                sql_query = self.read_query(path)
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    affected_rows += self.exec_query(cursor, sql_query, show_error=show_error, query_description=path)
            if explicit_commit:
                conn.commit()
        return affected_rows


    def exec_analyze_table(self, table_name, scheme='public'):
        """
        Perform the ANALYZE operation on a given table
        """
        # Detect if scheme is present in a table name
        if '.' not in table_name:
            table_name = '"{}"."{}"'.format(scheme, table_name)

        analyze_query = 'analyze {}'.format(table_name)
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                return self.exec_query(cursor, analyze_query, query_description='ANALYZE {}'.format(table_name))

    def exec_analyze_tables(self, table_names, scheme='public'):
        """
        Perform the ANALYZE operation on multiple tables
        """

        analyze_query = 'analyze {}'
        affected_rows = 0
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                for table_name in table_names:
                    # Detect if scheme is present in a table name
                    if '.' not in table_name:
                        table_name = '"{}"."{}"'.format(scheme, table_name)

                    affected_rows += self.exec_query(
                        cursor, analyze_query.format(table_name), query_description='ANALYZE {}'.format(table_name))
        return affected_rows


    def get_tables_list(self, scheme='public'):
        with get_postgres() as conn:
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
        with get_postgres() as conn:
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

runner = SqlRunner()