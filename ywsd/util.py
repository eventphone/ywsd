import asyncio
import functools
import logging
from importlib import import_module

from psycopg2 import OperationalError


def class_from_dotted_string(dotted_string):
    try:
        module_name, class_name = dotted_string.rsplit(".", 1)
    except ValueError as e:
        raise ImportError("Invalid dotted string: {}".format(dotted_string))

    module = import_module(module_name)

    try:
        return getattr(module, class_name)
    except AttributeError as e:
        raise ImportError(
            "There is no class {} in module {}".format(class_name, module_name)
        )


def retry_db_offline(count, wait_ms):
    def decorate(function):
        @functools.wraps(function)
        async def decorated(*args, **kwargs):
            for _ in range(count):
                try:
                    return await function(*args, **kwargs)
                except OperationalError as e:
                    logging.warning("Database error: {}. Waiting to retry...".format(e))
                await asyncio.sleep(wait_ms / 1000)
            logging.error("Continued database error. Stopped retrying....")

        return decorated

    return decorate
