"""
Load extracted and transformed data into a TSV file.
At the moment the TSV loader is the first step for all loaders.
"""
import os
import csv
import gzip
import inspect

from etl import constants
from etl.base.transformer import get_iterator_or_none, get_script_name


class TsvLoader:
    """
    Serialize flatten data into TSV files
    """

    def __init__(self, extractor, transformer):
        self.out_file = transformer.get_tsv_file()
        self.fields = transformer.get_tsv_fields()
        self.extractor = extractor
        self.transformer = transformer

    def _get_writer(self, compression=False):
        """
        Return Python independent file writer with compression support.
        If compression is enabled .gz prefix is added if it isn't in file_name

        :param compression: Use compression or not
        :return: tuple (file_name, file_writer)
        """
        file_writer = open
        file_writer_args = {'mode': 'wt', 'encoding': 'utf-8'}
        file_name = self.out_file

        if compression:
            if not file_name.endswith(constants.GZIP_EXT):
                file_name += constants.GZIP_EXT
            file_writer = gzip.open

        return file_name, file_writer(file_name, **file_writer_args)

    def load(self, *args, **kwargs):
        """
        :return: saved TSV file name or None
        """
        extracted_items_count = 0
        processed_rows_count = 0
        malformed_rows_count = 0

        # This is a very hackish way to fix the metrics name issue because of using threads instead of processes
        stack = inspect.stack()
        top_stack_frame = stack[-1]
        if 'threading' in top_stack_frame.filename[2:-3]:
            stack_frame = stack[2]
            script_name = os.path.basename(stack_frame.filename)
            script_name = script_name[:-3]
        else:
            script_name = get_script_name()

        extracted_items = get_iterator_or_none(self.extractor.extract())

        out_file, file_writer = self._get_writer(True)

        with file_writer as tsv_file:
            writer = csv.DictWriter(
                tsv_file, fieldnames=self.fields, delimiter='\t', quoting=csv.QUOTE_NONE, escapechar='\\')
            writer.writeheader()

            for item in extracted_items:
                for fact_row in self.transformer.transform(item):
                    try:
                        writer.writerow(fact_row)
                        processed_rows_count += 1
                    except Exception:
                        malformed_rows_count += 1
                        continue

                # Count only correctly processed items
                extracted_items_count += 1


        return out_file
