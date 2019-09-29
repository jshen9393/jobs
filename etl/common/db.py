"""
PostGresDB related utilities
"""
import sys
from time import sleep
from functools import wraps
from contextlib import contextmanager

import psycopg2
import psycopg2.extensions as extensions

from etl import config


class DbConn:
    """
    Safely obtain a DB connection and cache it for future uses. Thread safe.
    """

    def __init__(self):
        self._pool = []

        self._db_user = config.PG_DB_USER
        self._db_pass = config.PG_DB_PASS
        self._db_host = config.PG_DB_HOST
        self._db_port = config.PG_DB_PORT
        self._db_name = config.PG_DB_NAME

    def _connect(self):
        retries = config.ETL_DB_RETRIES
        while retries > 0:
            retries -= 1
            try:
                return psycopg2.connect(
                    host=self._db_host,
                    port=self._db_port,
                    dbname=self._db_name,
                    user=self._db_user,
                    password=self._db_pass,
                )
            except psycopg2.OperationalError:
                if retries == 0:
                    sys.exit(1)

                # Get some extra progressive sleep
                sleep(config.ETL_DB_TIMEOUT * (config.ETL_DB_RETRIES - retries))

    def get_conn(self):
        try:
            return self._pool.pop()
        except IndexError:
            return self._connect()

    def put_conn(self, conn):
        # Put back into the pool only good connections
        if not conn.closed:
            status = conn.get_transaction_status()
            if status != extensions.TRANSACTION_STATUS_UNKNOWN and status == extensions.TRANSACTION_STATUS_IDLE:
                self._pool.append(conn)


_DB_CONN = DbConn()


@contextmanager
def get_postgres():
    """
    Get Postgres connection
    """
    conn = _DB_CONN.get_conn()
    try:
        yield conn
    finally:
        _DB_CONN.put_conn(conn)


def handle_error(func):
    """
    Handle Postgres errors
    """
    @wraps(func)
    def _exec_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (psycopg2.DatabaseError, psycopg2.OperationalError) as ex:
            logger.error('PostgreSQL error %s', str(ex))
            raise

    return _exec_func
