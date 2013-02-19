# -*- encoding: utf-8 -*-

# Copyright 2013 Medium Entertainment, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
pyermc is a memcache client library with support for "pluggable"
driver backends.

Current backends include a text protocol driver (default), a binary
protocol driver, and a wrapped ultramemcached driver.

While pyermc is somewhat compatible with python-memcached, full
compatibility is not a design goal.

pyermc supports connecting to a single memcached instance (or server
that speaks the memcached protocol). If you require multiple servers,
consistent hashing, etc, then it is recommended to use twemproxy or
similar.

pyermc exposes connectivity faults, via exceptions, to the calling
code. This is in contrast to python-memcached, which simply enters an
'ignore backend' state. Exposing underlying faults is often needed when
working with queueing servers that support the memcache driver (such as
kestrel and darner), as well as translation proxies like couchbase's
moxie. If the old behavior is desired, there is an optional error_as_miss
parameter that may be set as part of client creation.
"""

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
    Client, MAX_KEY_LENGTH, MAX_VALUE_LENGTH,
    MemcacheKeyError, MemcacheValueError, MemcacheDriverException)
