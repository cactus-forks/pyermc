# -*- coding: utf8 -*-

# Copyright 2012 Mixpanel, Inc.
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

# Inspired from python-memcache:
#   http://www.tummy.com/Community/software/python-memcached/
# Ideas from python-ultramemcache:
#   https://github.com/nicholasserra/python-ultramemcached

import umemcache
import lz4
import cPickle as pickle
import logging


SERVER_MAX_KEY_LENGTH = 250
# memcached max is 1MB
# ultramemcache (driver specific max size) is  1MiB.
SERVER_MAX_VALUE_LENGTH = 1000000


class MemcacheKeyError(Exception):
    pass


class MemcacheValueError(Exception):
    pass


class Client(object):
    _FLAG_PICKLE     = 1<<0
    _FLAG_INTEGER    = 1<<1
    _FLAG_LONG       = 1<<2
    _FLAG_COMPRESSED = 1<<3

    def __init__(self, host, port, connect_timeout=3, timeout=3,
            server_max_key_length=SERVER_MAX_KEY_LENGTH,
            server_max_value_length=SERVER_MAX_VALUE_LENGTH,
            pickle=True, cache_cas=False, debug=False):
        super(Client, self).__init__()
        self.host = host
        self.port = port
        self.timeout = timeout
        self.connect_timeout = connect_timeout

        self.server_max_key_length = server_max_key_length
        self.server_max_value_length = server_max_value_length

        self.pickle = pickle
        self.debug = debug
        self.cache_cas = cache_cas

        self._client = None
        self._connected = False
        self.cas_ids = {}

    def connect(self, reconnect=False):
        if self._connected and not reconnect:
            return

        if self._connected and reconnect:
            self._client.close()
            self._client = None

        if not self._client:
            self._client = umemcache.Client("%s:%s" % (self.host, self.port))

        self._client.sock.settimeout(self.connect_timeout)
        self._client.connect()
        self._client.sock.settimeout(self.timeout)
        self._connected = True

    def close(self):
        self._client.close()
        self._client = None
        self._connected = False

    def is_connected(self):
        if not self._connected:
            return False
        if not self._client:
            self._connected = False
            return False
        if not self._client.is_connected():
            self._connected = False
            self._client = None
            return False

    # alias close to disconnect
    disconnect = close

    def reset_client(self):
        self.reset_cas()

    def reset_cas(self):
        self.cas_ids = {}

    def check_key(self, key, key_extra_len=0):
        if isinstance(key, tuple):
            key = key[1]
        if not key:
            raise MemcacheKeyError("Key is None")
        if isinstance(key, unicode):
            # try to encode to utf8
            try:
                key = key.encode('utf-8')
            except:
                raise MemcacheKeyError(
                    "Key was unicode, and failed to convert to str with "
                    "key.encode('utf-8')")
        if not isinstance(key, str):
            raise MemcacheKeyError("Key must be a str")

        if isinstance(key, basestring):
            if self.server_max_key_length != 0 and \
                    len(key) + key_extra_len > self.server_max_key_length:
                raise MemcacheKeyError(
                    "Key length is > %s" % self.server_max_key_length)
            for char in key:
                if ord(char) < 33 or ord(char) == 127:
                    raise MemcacheKeyError(
                            "Control characters not allowed")
        return key

    def stats(self):
        return self._client.stats()

    def version(self):
        return self._client.version()

    def decr(self, key, increment=1):
        return self._client.decr(key, increment)

    def incr(self, key, increment=1):
        return self._client.incr(key, increment)

    def delete(self, key):
        return self._client.delete(key)

    def flush_all(self):
        return self._client.flush_all()

    ##
    ## set operations
    ##
    def add(self, key, val, time=0, min_compress_len=0):
        self._set("add", key, val, time, min_compress_len)

    def append(self, key, val, time=0, min_compress_len=0):
        self._set("append", key, val, time, min_compress_len)

    def prepend(self, key, val, time=0, min_compress_len=0):
        self._set("prepend", key, val, time, min_compress_len)

    def replace(self, key, val, time=0, min_compress_len=0):
        self._set("replace", key, val, time, min_compress_len)

    def set(self, key, val, time=0, min_compress_len=0):
        self._set("set", key, val, time, min_compress_len)

    #def set_multi(self):
    #    ## unsupport!

    def cas(self, key, val, time=0, min_compress_len=0):
        self._set("cas", key, val, time, min_compress_len)

    ## from python-memcache
    def _val_to_store_info(self, val, min_compress_len):
        """
        Transform val to a storable representation, returning a tuple of the
        flags, the length of the new value, and the new value itself.
        """
        flags = 0
        if isinstance(val, str):
            pass
        elif isinstance(val, int):
            flags |= Client._FLAG_INTEGER
            val = "%d" % val
            # force no attempt to compress this silly string.
            min_compress_len = 0
        elif isinstance(val, long):
            flags |= Client._FLAG_LONG
            val = "%d" % val
            # force no attempt to compress this silly string.
            min_compress_len = 0
        else:
            if self.pickle:
                flags |= Client._FLAG_PICKLE
                val = pickle.dumps(val)

        lv = len(val)
        # We should try to compress if min_compress_len > 0 and we could
        # import zlib and this string is longer than our min threshold.
        if min_compress_len and lv > min_compress_len:
            comp_val = lz4.compress(val)
            # Only retain the result if the compression result is smaller
            # than the original.
            if len(comp_val) < lv:
                flags |= Client._FLAG_COMPRESSED
                val = comp_val

        #  silently do not store if value length exceeds maximum
        lzv = len(val)
        if self.server_max_value_length != 0 and \
                lzv > self.server_max_value_length:
            raise MemcacheValueError(
                "Value is larger than configured max_value_length. %d > %d" %
                (lzv, self.server_max_value_length))

        return (flags, lzv, val)

    def _set(self, cmd, key, val, time=0, min_compress_len=0):
        key = self.check_key(key)
        if not self.is_connected():
            self.connect()

        flags, sval_len, sval = self._val_to_store_info(val, min_compress_len)

        if cmd == 'cas':
            if key not in self.cas_ids:
                # not in cas, so just do a set
                return self._set('set', key, val, time, min_compress_len)
            args = (key, sval, self.cas_ids[key], time, flags)
        else:
            args = (key, sval, time, flags)
        return getattr(self._client, cmd)(*args)

    ##
    ## get operations
    ##
    def get(self, key):
        return self._get("get", key)

    def get_multi(self, keys):
        return self._get_multi('get_multi', keys)

    def gets(self, key):
        return self._get("gets", key)

    def gets_multi(self, keys):
        return self._get_multi('gets_multi', keys)

    def _recv_value(self, buf, flags):
        if flags & Client._FLAG_COMPRESSED:
            buf = lz4.decompress(buf)

        if  flags == 0 or flags == Client._FLAG_COMPRESSED:
            # Either a bare string or a compressed string now decompressed...
            val = buf
        elif flags & Client._FLAG_INTEGER:
            val = int(buf)
        elif flags & Client._FLAG_LONG:
            val = long(buf)
        elif flags & Client._FLAG_PICKLE:
            val = pickle.loads(buf)
        return val

    def _get(self, cmd, key):
        key = self.check_key(key)
        if not self.is_connected():
            self.connect()

        response = getattr(self._client, cmd)(key)
        if not response:
            return None

        if cmd == 'gets':
            val, flags, cas_id = response
            if self.cache_cas:
                self.cas_ids[key] = cas_id
        else:
            val, flags = response

        if not val:
            return None

        value = self._recv_value(val, flags)
        return value

    def _get_multi(self, cmd, keys):
        checked_keys = []
        for key in keys:
            checked_keys.append(self.check_key(key))
        keys = checked_keys
        if not self.is_connected():
            self.connect()

        response = getattr(self._client, cmd)(keys)
        retvals = {}
        for k in response:
            if cmd == 'gets_multi':
                value, flags, cas_id = response[k]
                if self.cache_cas:
                    self.cas_ids[k] = cas_id
            else:
                value, flags = response[k]
            val = self._recv_value(value, flags)
            retvals[k] = val
        return retvals
