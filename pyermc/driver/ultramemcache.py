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
Memcache Ultramemcache (umemcache) backend
"""

from .base import Driver
import umemcache
import errno
import socket


class UMemcacheDriver(Driver):
    def __init__(self, host, port, timeout, connect_timeout,
                 disable_nagle=True):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.disable_nagle = disable_nagle
        self._client = None
        self._connected = False

    def connect(self, reconnect=False):
        if self.is_connected():
            if not reconnect:
                return
            self.close()

        self._client = umemcache.Client("%s:%s" % (self.host, self.port))
        self._client.sock.settimeout(self.connect_timeout)
        self._client.connect()
        self._client.sock.settimeout(self.timeout)
        if self.disable_nagle:
            # disable nagle, as memcache deals with lots of small packets.
            self._client.sock.setsockopt(
                socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._connected = True

    def is_connected(self):
        if not self._connected:
            return False
        if not self._client:
            self._connected = False
            return False
        if not hasattr(self._client, 'sock'):
            self._connected = False
            self._client = None
            return False
        if not self._client.is_connected():
            self._connected = False
            self._client = None
            return False
        return True
        ## this is arguably safer, but it slows things down a
        ## non-insignificant amount. Consider putting this into pooling
        ## code instead of per-memcache call
        #try:
        #    self._client.sock.settimeout(0)
        #    self._client.sock.recv(1, socket.MSG_PEEK)
        #    # if recv didn't raise, then the socket was closed or there
        #    # is junk in the read buffer, either way, close
        #    self.close()
        #except socket.error as e:
        #    # this is expected if the socket is still open
        #    if e.errno == errno.EAGAIN:
        #        self._client.sock.settimeout(self.timeout)
        #        return True
        #    else:
        #        self.close()
        #return False

    def close(self):
        if self._client:
            self._client.close()
        self._client = None
        self._connected = False

    def stats(self):
        if not self.is_connected():
            self.connect()
        return self._client.stats()

    def version(self):
        if not self.is_connected():
            self.connect()
        return self._client.version()

    def flush_all(self):
        if not self.is_connected():
            self.connect()
        return self._client.flush_all()

    def delete(self, key):
        if not self.is_connected():
            self.connect()
        response = self._client.delete(key)
        if response == 'NOT_FOUND':
            return False
        return True

    def incr(self, key, val):
        if not self.is_connected():
            self.connect()
        response = self._client.incr(key, val)
        if response == 'NOT_FOUND':
            return None
        return response

    def decr(self, key, val):
        if not self.is_connected():
            self.connect()
        response = self._client.decr(key, val)
        if response == 'NOT_FOUND':
            return None
        return response

    def cas(self, key, val, cas_id, time, flags):
        if not self.is_connected():
            self.connect()
        return self._client.cas(key, val, cas_id, time, flags)

    def get(self, key):
        if not self.is_connected():
            self.connect()
        return self._client.get(key)

    def gets(self, key):
        if not self.is_connected():
            self.connect()
        return self._client.gets(key)

    def get_multi(self, keys):
        if not self.is_connected():
            self.connect()
        return self._client.get_multi(keys)

    def gets_multi(self, keys):
        if not self.is_connected():
            self.connect()
        return self._client.gets_multi(keys)

    def add(self, key, val, time, flags):
        if not self.is_connected():
            self.connect()
        response = self._client.add(key, val, time, flags)
        if response == 'STORED':
            return True
        return False

    def append(self, key, val, time, flags):
        if not self.is_connected():
            self.connect()
        response = self._client.append(key, val, time, flags)
        if response == 'STORED':
            return True
        return False

    def prepend(self, key, val, time, flags):
        if not self.is_connected():
            self.connect()
        response = self._client.prepend(key, val, time, flags)
        if response == 'STORED':
            return True
        else:
            return False

    def replace(self, key, val, time, flags):
        if not self.is_connected():
            self.connect()
        response = self._client.replace(key, val, time, flags)
        if response == 'STORED':
            return True
        return False

    def set(self, key, val, time, flags):
        if not self.is_connected():
            self.connect()
        response = self._client.set(key, val, time, flags)
        if response == 'STORED':
            return True
        return False
