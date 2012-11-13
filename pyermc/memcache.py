# -*- coding: utf8 -*-

# Copyright 2012 PlayHaven, Inc.
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

# Inspiration from python-memcache:
#   http://www.tummy.com/Community/software/python-memcached/
# Inspiration from python-ultramemcache:
#   https://github.com/nicholasserra/python-ultramemcached
# Inspiration from memcache_client:
#   https://github.com/mixpanel/memcache_client

import umemcache
import lz4
import cPickle as pickle
import logging
import socket
import errno
import re


CONNECT_TIMEOUT = 3
SOCKET_TIMEOUT = 3
MAX_KEY_LENGTH = 250
# memcached max is 1MB, but
# ultramemcache (driver specific max size) is 1MiB.
MAX_VALUE_LENGTH = 1000000


class MemcacheKeyError(Exception):
    pass


class MemcacheValueError(Exception):
    pass


class Client(object):
    # bitfields
    _FLAG_PICKLE     = 1<<0
    _FLAG_INTEGER    = 1<<1
    _FLAG_LONG       = 1<<2
    _FLAG_COMPRESSED = 1<<3
    # regex for key validation
    _valid_key_re = re.compile('^[^\x00-\x20\x7f\n\s]+$')

    def __init__(self, host, port,
            connect_timeout=CONNECT_TIMEOUT,
            timeout=SOCKET_TIMEOUT,
            max_key_length=MAX_KEY_LENGTH,
            max_value_length=MAX_VALUE_LENGTH,
            pickle=True, disable_nagle=True,
            cache_cas=False, error_as_miss=False):
        super(Client, self).__init__()
        self.host = host
        self.port = port

        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.max_key_length = max_key_length
        self.max_value_length = max_value_length

        self.pickle = pickle
        self.disable_nagle = disable_nagle
        self.cache_cas = cache_cas
        self.error_as_miss = error_as_miss

        self._client = None
        self._connected = False
        self.cas_ids = {}

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

    def close(self):
        if self._client:
            self._client.close()
        self._client = None
        self._connected = False

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

    # alias close to disconnect
    disconnect = close

    def reset_client(self):
        self.reset_cas()

    def reset_cas(self):
        self.cas_ids = {}

    ##
    ## misc operations
    ##
    def stats(self):
        return self._send_cmd("stats")

    def version(self):
        return self._send_cmd("version")

    def incr(self, key, increment=1):
        return self._send_cmd("incr", key, increment)

    def decr(self, key, decrement=1):
        return self._send_cmd("decr", key, decrement)

    def delete(self, key):
        return self._send_cmd("delete", key)

    def flush_all(self):
        return self._send_cmd("flush_all")

    ##
    ## set operations
    ##
    def add(self, key, val, time=0, min_compress_len=0):
        return self._set("add", key, val, time, min_compress_len)

    def append(self, key, val, time=0, min_compress_len=0):
        return self._set("append", key, val, time, min_compress_len)

    def prepend(self, key, val, time=0, min_compress_len=0):
        return self._set("prepend", key, val, time, min_compress_len)

    def replace(self, key, val, time=0, min_compress_len=0):
        return self._set("replace", key, val, time, min_compress_len)

    def set(self, key, val, time=0, min_compress_len=0):
        return self._set("set", key, val, time, min_compress_len)

    #def set_multi(self):
    #    ## unsupport!

    def cas(self, key, val, time=0, min_compress_len=0):
        return self._set("cas", key, val, time, min_compress_len)

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

    ##
    ## data massaging methods
    ##
    def check_key(self, key):
        if not key:
            raise MemcacheKeyError("Key is None")
        if not isinstance(key, str):
            if isinstance(key, unicode):
                # we got unicode. try to utf8 encode to str
                try:
                    key = key.encode('utf-8')
                except:
                    raise MemcacheKeyError(
                        "Key was unicode, and failed to convert to str with "
                        "key.encode('utf-8')")
            else:
                raise MemcacheKeyError("Key must be a str")

        lk = len(key)
        if self.max_key_length and lk > self.max_key_length:
            raise MemcacheKeyError("Key length is > %s" % self.max_key_length)

        #for c in key:
        #    oc = ord(c)
        #    if oc < 33 or oc == 127:
        #        raise MemcacheKeyError("Control characters not allowed")
        m = self._valid_key_re.match(key)
        if m:
            # in python re, $ matches either end of line or right before
            # \n at end of line. We can't allow latter case, so
            # making sure length matches is simplest way to detect
            if len(m.group(0)) != lk:
                raise MemcacheKeyError("Control characters not allowed")
        else:
            raise MemcacheKeyError("Control characters not allowed")
        return key

    # flag logic from python-memcache
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
            # maxint is pretty tiny. just return
            return (flags, val)
        elif isinstance(val, long):
            flags |= Client._FLAG_LONG
            val = "%d" % val
            # longs can be huge, so check length and compress if long enough
        else:
            if self.pickle:
                flags |= Client._FLAG_PICKLE
                val = pickle.dumps(val)

        lv = len(val)
        #  silently do not store if value length exceeds maximum
        if self.max_value_length and lv > self.max_value_length:
            raise MemcacheValueError(
                "Value is larger than configured max_value_length. %d > %d" %
                (lv, self.max_value_length))

        # We should try to compress if min_compress_len > 0 and this
        # string is longer than min threshold.
        if min_compress_len and lv > min_compress_len:
            comp_val = lz4.compress(val)
            # Only actually compress if the compressed result is smaller
            # than the original.
            if len(comp_val) < lv:
                flags |= Client._FLAG_COMPRESSED
                val = comp_val
        return (flags, val)

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

    ##
    ## client calling methods
    ##
    def _set(self, cmd, key, val, time=0, min_compress_len=0):
        key = self.check_key(key)
        flags, sval = self._val_to_store_info(val, min_compress_len)

        args = (key, sval, time, flags)
        if cmd == 'cas':
            if key in self.cas_ids:
                args = (key, sval, self.cas_ids[key], time, flags)
            else:
                cmd = 'set'  # key not in cas_ids, so just do a set instead
        return self._send_cmd(cmd, *args)

    def _get(self, cmd, key):
        key = self.check_key(key)
        response = self._send_cmd(cmd, key)
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
        keys = [self.check_key(k) for k in keys]
        response = self._send_cmd(cmd, keys)
        if not response:
            return {}

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

    def _send_cmd(self, cmd, *args):
        if not self.is_connected():
            self.connect()
        try:
            return getattr(self._client, cmd)(*args)
        except (RuntimeError, IOError) as e:
            self.close()
            if self.error_as_miss:
                return None
            raise
