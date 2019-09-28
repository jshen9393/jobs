"""
Load a TSV file into a Redshift database.
"""
import os
import re
import inspect

import psycopg2
import psycopg2.extras

from etl import config
from etl.constants import NULL_VALUE
from etl.sql.runner import runner
from etl.common.db import get_redshift, handle_error
from etl import constants

REDSHIFT_COPY_ERROR = """"\
    File: '{filename}'
    Error: '{err_reason}'
    FieldName: '{colname}'
    FieldType: '{type}'
    ReceivedValue: '{raw_field_value}'
    RawLine: '{raw_line}' 
"""


class PostGresLoader:
    """
    Redshift data loader
    """

    def __init__(self, transformer, file):
        self.table_name = transformer.get_stage_table_name()
        self.table_ddl = transformer.get_stage_table_ddl()
        self.file = file.get_file_name()

    @handle_error
    def rebuild_stage_table(self, recreate_table=True):
        """
        Rebuild appropriate ETL stage table
        """
        with get_redshift() as conn:
            with conn.cursor() as cursor:
                if recreate_table:
                    cursor.execute("drop table if exists {} cascade".format(self.table_name))
                    cursor.execute(self.table_ddl)
                    conn.commit()
                    return

                if not runner.table_exists(self.table_name):
                    cursor.execute(self.table_ddl)
                    conn.commit()
                    return

    def load(self, *args, **kwargs):

        with get_redshift() as conn:
            with conn.cursor() as cursor:
                try:
                    with open(constants.LOCAL_STORAGE+self.file,"r") as upload_file:
                        # cursor.copy_from(upload_file,self.table_name,sep='\t')
                        # conn.commit()
                        cursor.copy_expert("COPY {} from stdin with csv header delimiter '\t'"
                                           .format(self.table_name),upload_file)
                        conn.commit()


                    upload_error = False
                    for msg in conn.notices:
                        if '{}'.format(self.table_name) not in msg:
                            continue

                        upload_error = True if 'stl_load_errors' in msg else upload_error
                        if 'record' in msg:
                            try:
                                rows_cnt = int(re.findall(r'\d+', msg)[0])
                            except (IndexError, ValueError):
                                pass

                    if upload_error:
                        self.show_redshift_error(conn, rollback=False)
                        return

                except psycopg2.InternalError as ex:
                    raise ex

    def show_redshift_error(self, conn, rollback=True):
        """
        Fetch data load error from Redshift specific tables
        """
        if rollback:
            conn.rollback()

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute('SELECT pg_last_copy_id() as query_id')
            query_id = cursor.fetchone()['query_id']
            cursor.execute("""
                SELECT
                  "raw_line",
                  "filename",
                  "line_number",
                  "colname",
                  "type",
                  "raw_field_value",
                  "err_reason"
                FROM pg_catalog.stl_load_errors
                WHERE "query" = {};
                """.format(query_id))

            errors = cursor.fetchall()
            formatted_errors = []
            for row in errors:
                err_row = dict((key, str(val).strip()) for key, val in row.items())
                formatted_errors.append(REDSHIFT_COPY_ERROR.format(**err_row))

            msg = 'Redshift: stl_load_errors:{}'.format('\n'.join(formatted_errors))
            self.logger.error(msg)
            return errors
