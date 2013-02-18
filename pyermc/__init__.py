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
