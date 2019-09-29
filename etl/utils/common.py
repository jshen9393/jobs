"""
Common utilities
"""
import os
import sys
import time
import json
import socket
import itertools
from concurrent.futures import ThreadPoolExecutor, as_completed

from etl import config


def exec_time(func):
    """
    Measure execution time
    """
    def _timed(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        return result
    return _timed


def exec_time_info(info_str):
    """
    Another version of timing decorator which allows info string as a parameter
    """
    def _timing_decorator(func):
        def _timed(*args, **kwargs):
            start = time.time()
            msg = "Start '{}'".format(info_str)
            result = func(*args, **kwargs)
            msg = "Finished '{}' in {:.2f}s".format(info_str, time.time() - start)
            return result
        return _timed
    return _timing_decorator


def class_deprecated(cls):
    """
    Show a deprecation warning
    """
    msg = "Class '{}.{}' is deprecated".format(cls.__module__, cls.__name__)
    logger.warning(msg)
    return cls


def get_iterator_or_none(iterator, empty_iterator=False):
    """
    Return None if empty iterator received.
    This function created to use conditions with iterators, for ex.:
        stream_data = get_iterator_or_none(stream_data)
        if stream_data is None:
            break
    """
    for item in iterator:
        return itertools.chain((item, ), iterator)

    # return empty iterator instead of None
    if empty_iterator:
        return ()


def get_script_name():
    """
    Get nice looking script name with stripped path and .py
    """
    return os.path.basename(sys.argv[0]).split('.')[0]


def get_cmd_line():
    """
    NOTE: Here command line is not a true cmd line. It is used for the logging message only
    """
    return ' '.join((get_script_name(), ' '.join(sys.argv[1:])))


def get_etl_env():
    """
    Get ETL specific environment variables
    """
    return json.dumps({
        key: value for key, value in os.environ.items() if key.startswith('ETL_') or key.startswith('SQL_')})


def get_elapsed_time(started_at, finished_at):
    """
    Calculate elapsed time between two datetime objects.

    :param datetime.datetime started_at:
    :param datetime.datetime finished_at:
    :return:
    """
    delta = finished_at - started_at
    return delta.seconds + delta.microseconds / 1000000.


def get_host_name():
    """
    Get host name.
    TODO: Cache the hostname value?
    """
    try:
        return socket.gethostname()
    except OSError as ex:
        return str(ex)


def jprint(anything, print_type=False):
    """
    Pretty print based on JSON.
    """
    if print_type:
        print('Anything type is: {}'.format(type(anything)))

    print(json.dumps(anything, default=str, indent=4))


# #### for future use of threading
# def thread_executor(targets, max_workers=config.ETL_THREAD_POOL_SIZE):
#     """
#     Execute given list of functions in separated threads.
#     This executor blocks until all threads finish.
#
#     targets is a list of dicts which have all required parameters to run a thread
#     """
#     with ThreadPoolExecutor(max_workers=max_workers) as executor:
#         futures = []
#         for target in targets:
#             futures.append(executor.submit(target.pop('target'), **target.pop('kwargs')))
#
#         for future in as_completed(futures):
#             future.result()


