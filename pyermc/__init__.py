from ._version import __version__

## avoid "no handler found" warnings
import logging
try:
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())

# clean up
del logging
del NullHandler

from .memcache import (
    Client, MemcacheKeyError, MemcacheValueError,
    SERVER_MAX_KEY_LENGTH, SERVER_MAX_VALUE_LENGTH)
