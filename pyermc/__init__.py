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
    MAX_KEY_LENGTH, MAX_VALUE_LENGTH)
