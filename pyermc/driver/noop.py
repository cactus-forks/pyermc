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

from .base import Driver


class NoopDriver(Driver):
    def __init__(self, host, port, timeout, connect_timeout,
                 disable_nagle=True):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.disable_nagle = disable_nagle

    ###
    ### connection handling
    def connect(self, reconnect=False):
        pass

    def is_connected(self):
        return True

    def close(self):
        pass

    ###
    ### exposed driver methods
    def stats(self):
        return {}

    def version(self):
        return "0.0.0"

    def flush_all(self):
        return

    def delete(self, key):
        return

    def incr(self, key, val):
        return val+1

    def decr(self, key, val):
        return val-1

    def cas(self, key, val, cas_id, time, flags):
        return True

    def get(self, key):
        return None

    def gets(self, key):
        return None

    def get_multi(self, keys):
        return {}

    def gets_multi(self, keys):
        return {}

    def add(self, key, val, time, flags):
        return True

    def append(self, key, val, time, flags):
        return True

    def prepend(self, key, val, time, flags):
        return True

    def replace(self, key, val, time, flags):
        return True

    def set(self, key, val, time, flags):
        return True
