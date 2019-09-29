"""
Base ETL context class.
"""
from datetime import datetime

from etl import constants


class BaseTransformer:
    """
    The ETL context class provides methods to get some ETL context information like output TSV file name,
    TSV fields, stage table names, stage tables DDL etc.
    """
    _stage_table_name = None
    _stage_table_ddl = None
    _tsv_fields = None

    @staticmethod
    def _make_tsv_file_name(name):
        tsv_file_name = constants.TSV_OUTPUT_FILE_PATTERN.format(
            name=name,
            timestamp=datetime.utcnow().strftime(constants.TSV_OUTPUT_TIMESTAMP),
        )

        return tsv_file_name

    def get_stage_table_name(self):
        """
        Get stage table name
        """
        if self._stage_table_name:
            return self._stage_table_name

        raise NotImplementedError('The get_stage_table_name() method must be implemented')

    def get_stage_table_ddl(self):
        """
        Get stage table DDL SQL
        """
        if self._stage_table_ddl:
            return self._stage_table_ddl

        raise NotImplementedError('The get_stage_table_ddl() method must be implemented')

    def get_tsv_fields(self):
        """
        Get list of TSV fields
        """
        if self._tsv_fields:
            return self._tsv_fields

        raise NotImplementedError('The get_tsv_fields() method must be implemented')

    def get_tsv_file(self):
        """
        Get output TSV file name
        """
        if self._stage_table_name:
            return self._make_tsv_file_name(self._stage_table_name)

        raise NotImplementedError('The get_tsv_file() method must be implemented')


