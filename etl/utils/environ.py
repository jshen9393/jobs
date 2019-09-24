"""
Helper functions to get various config parameters from the environment.
"""
import os

from etl.constants import TRUE_VALUES

__all__ = [
    'get_str_value',
    'get_int_value',
    'get_bool_value',
    'get_float_value',
]


def _get_value(env_var, default=None, func=None, show_error=False):
    value = os.environ.get(env_var, default)
    if callable(func):
        try:
            value = func(value)
        except ValueError:
            # TODO: Should an error be shown here?
            if show_error:
                print("ENV ERROR: Received '{}' for '{}'. Using default '{}'".format(value, env_var, default))
            return default

    return value


def get_str_value(env_var, default='', show_error=False):
    """
    Get environment value as a string
    """
    return _get_value(env_var, default=default, show_error=show_error)


def get_int_value(env_var, default=0, show_error=False):
    """
    Get environment value as a int
    """
    return _get_value(env_var, default=default, func=int, show_error=show_error)


def get_bool_value(env_var, default=False, show_error=False):
    """
    Get environment value as a bool
    """
    value = _get_value(env_var, default=default, show_error=show_error)
    if isinstance(value, str):
        value = value.lower()
    return value in TRUE_VALUES


def get_float_value(env_var, default=0.0, show_error=False):
    """
    Get environment value as a float
    """
    return _get_value(env_var, default=default, func=float, show_error=show_error)
