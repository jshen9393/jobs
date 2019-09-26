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

REDSHIFT_COPY_ERROR = """"\
    File: '{filename}'
    Error: '{err_reason}'
    FieldName: '{colname}'
    FieldType: '{type}'
    ReceivedValue: '{raw_field_value}'
    RawLine: '{raw_line}' 
"""


class RedshiftLoader:
    """
    Redshift data loader
    """

    def __init__(self, transformer, s3_loader=None):
        self.table_name = transformer.get_stage_table_name()
        self.table_ddl = transformer.get_stage_table_ddl()
        self.s3_file = s3_loader.get_s3_file()

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

    @exec_time
    def load(self, *args, **kwargs):
        """
        Load data from S3 into a stage table.

        Optional parameter:
        s3_file - S3 full file name or prefix
        """
        s3_file = kwargs.pop('s3_file', None) or self.s3_file
        if not s3_file:
            self.logger.warning('S3 prefix is empty. Skip COPY operation')
            return

        with get_redshift() as conn:
            # build access string for Redshift
            access_string = " access_key_id '{}' secret_access_key '{}' ".format(
                config.AWS_ACCESS_KEY_ID, config.AWS_SECRET_ACCESS_KEY,
            )
            # NOTE: If the role is used then it must be created and
            # assigned to Redshift in advance at the moment.
            if config.AWS_REDSHIFT_S3_ACCESS_ROLE is not None:
                access_string = " iam_role '{}' ".format(config.AWS_REDSHIFT_S3_ACCESS_ROLE)

            with conn.cursor() as cursor:
                # This is a very hackish way to fix the metrics name issue because of using threads instead of processes
                stack = inspect.stack()
                top_stack_frame = stack[-1]
                if 'threading' in top_stack_frame.filename[2:-3]:
                    stack_frame = stack[2]
                    script_name = os.path.basename(stack_frame.filename)
                    script_name = script_name[:-3]
                else:
                    script_name = get_script_name()

                try:
                    msg = "Redshift: COPY from file '{}'".format(s3_file)
                    self.logger.info(msg)
                    cursor.execute("""
                        copy {db_table}
                        from '{s3_file_or_prefix}'
                        {access_string}
                        delimiter '\t'
                        NULL AS '{null_string}'
                        ESCAPE
                        IGNOREHEADER 1
                        COMPUPDATE OFF
                        STATUPDATE OFF
                        ENCODING UTF8
                        TRUNCATECOLUMNS
                        MAXERROR {max_allowed_errors}
                        {compressed}
                    """.format(
                        db_table=self.table_name,
                        s3_file_or_prefix=s3_file,
                        access_string=access_string,
                        null_string=NULL_VALUE,
                        max_allowed_errors=config.ETL_REDSHIFT_MAX_ERRORS,
                        compressed='gzip' if config.ETL_COMPRESS_TSV else ''))
                    conn.commit()

                    upload_error = False
                    rows_cnt = 0
                    for msg in conn.notices:
                        if '{}'.format(self.table_name) not in msg:
                            continue

                        upload_error = True if 'stl_load_errors' in msg else upload_error
                        msg = 'Redshift: {}'.format(msg[7:-1])
                        self.logger.info(msg)
                        if 'record' in msg:
                            try:
                                rows_cnt = int(re.findall(r'\d+', msg)[0])
                            except (IndexError, ValueError):
                                pass

                    if upload_error:
                        self.show_redshift_error(conn, rollback=False)
                        return

                    # AWS CW metrics
                    metric_name = '{}:redshift_rows'.format(script_name)
                    self.logger.put_metric(metric_name, rows_cnt, unit='Count')
                except psycopg2.InternalError as ex:
                    msg = 'Redshift: redshift upload errors limit ({}) reached or unexpected error'.format(
                        config.ETL_REDSHIFT_MAX_ERRORS) + '\n rollback last query'
                    self.logger.critical(msg)
                    self.show_redshift_error(conn)
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
