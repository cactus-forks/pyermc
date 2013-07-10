# -*- coding: utf8 -*-

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

# Inspiration from python-memcache:
#   http://www.tummy.com/Community/software/python-memcached/
# Inspiration from python-ultramemcache:
#   https://github.com/nicholasserra/python-ultramemcached
# Inspiration from memcache_client:
#   https://github.com/mixpanel/memcache_client

import lz4
import cPickle as pickle
import socket
import re
from . import driver


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


class MemcacheDriverException(Exception):
    pass


class MemcacheSocketException(Exception):
    pass


class Client(object):
    """
    Object representing a connection to a backend memcache protocol driver.
    """
    # bitfields
    _FLAG_PICKLE     = 1<<0
    _FLAG_INTEGER    = 1<<1
    _FLAG_LONG       = 1<<2
    _FLAG_COMPRESSED = 1<<3
    # regex for key validation
    _valid_key_re = re.compile('^[^\x00-\x20\x7f\n\s]+$')

    def __init__(self, host='127.0.0.1', port=11211,
                 connect_timeout=CONNECT_TIMEOUT,
                 timeout=SOCKET_TIMEOUT,
                 max_key_length=MAX_KEY_LENGTH,
                 max_value_length=MAX_VALUE_LENGTH,
                 pickle=True, pickle_proto=2,
                 disable_nagle=True, cache_cas=False, error_as_miss=False,
                 client_driver=driver.DEFAULT_DRIVER):
        """
        Create a new Client object connecting to the host and port.

        Keyword arguments:
          host             -- host to connect to
                              default: "127.0.0.1"
          port             -- port to connect to
                              default: 11211
          connect_timeout  -- how long to wait trying to connect
                              default: CONNECT_TIMEOUT
          timeout          -- how long to wait for a given protocol response
                              default: SOCKET_TIMEOUT
          max_key_length   -- max key size server will accept
                              defaut: MAX_KEY_LENGTH
          max_value_length -- max value size server will accept
                              defaut: MAX_VALUE_LENGTH
          pickle           -- whether to support pickling objects or not
                              default: True
          pickle_proto     -- pickle protocol to use. default: 2 (highest)
          disable_nagle    -- disable Nagle's algorithm for the tcp socket.
                              May help improve performance in some cases.
                              default: False
          cache_cas        -- whether `gets`/`gets_multi` commands store
                              CAS_ID internally, and `cas` commands perform
                              `cas` or simply fallback to `set`.
                              default: False
          error_as_miss    -- try to simulate python-memcache driver behavior
                              where a driver/socket fault simply returns
                              a `None`, and masks all errors.
                              default: False
          client_driver    -- backend driver class reference that must be a
                              a subclass of `pyermc.driver.Driver`.
                              default: pyermc.driver.TextProtoDriver
        """
        self.host = host
        self.port = port

        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.max_key_length = max_key_length
        self.max_value_length = max_value_length

        self.pickle = pickle
        self.pickle_proto = pickle_proto
        self.disable_nagle = disable_nagle
        self.cache_cas = cache_cas
        self.error_as_miss = error_as_miss

        self._client = None
        self.cas_ids = {}
        self._driver = None

        if client_driver and issubclass(client_driver, driver.Driver):
            self._driver = client_driver
        else:
            raise TypeError('Bad driver provided')
        self._init_driver()

    def __del__(self):
        ## try to close/cleanup on GC/delete
        try:
            if self._client:
                self.close()
        except:
            pass

    def _init_driver(self):
        self._client = self._driver(
            self.host, self.port,
            timeout=self.timeout,
            connect_timeout=self.connect_timeout,
            disable_nagle=self.disable_nagle)

    @property
    def socket(self):
        if self._client:
            return self._client.socket
        return None

    def connect(self, reconnect=False):
        """
        Connects the backend to the server

        Keyword arguments:
          reconnect -- Reconnect to the server if connected.
                       (defaut: False)
        """
        if not self._client:
            self._init_driver()
        self._client.connect(reconnect=reconnect)

    def is_connected(self):
        """
        Checks to see if the backend is connected

        returns bool
        """
        if not self._client:
            return False
        return self._client.is_connected()

    def close(self):
        """
        Closes connection to the backend
        """
        if self._client:
            self._client.close()
        self._client = None

    # alias close to disconnect
    disconnect = close

    def reset_client(self):
        """
        Reset internal client state
        """
        self.reset_cas()

    def reset_cas(self):
        """
        Reset internal CAS associations
        """
        self.cas_ids = {}

    ##
    ## misc operations
    ##
    def stats(self):
        """
        Get stats from backend server

        returns dict
        """
        return self._call_driver("stats")

    def version(self):
        """
        Get version from backend server

        returns string
        """
        return self._call_driver("version")

    def incr(self, key, delta=1):
        """
        Increment a stored number identified by `key`

        Arguments:
         key   -- string key

        Keyword Arguments:
         delta -- value to increment by. default:1

        returns int or None (on failure)
        """
        if not isinstance(delta, int):
            raise TypeError("An integer is required")
        if delta < 0:
            return self._call_driver("decr", key, abs(delta))
        return self._call_driver("incr", key, delta)

    def decr(self, key, delta=1):
        """
        Decrement a stored number identified by `key`

        Arguments:
         key   -- string key

        Keyword Arguments:
         delta -- value to decrement by. default:1

        returns int or None (on failure)
        """
        if not isinstance(delta, int):
            raise TypeError("An integer is required")
        if delta < 0:
            return self._call_driver("incr", key, abs(delta))
        return self._call_driver("decr", key, delta)

    def delete(self, key):
        """
        Delete a stored value identified by `key`

        Arguments:
          key  -- string key

        returns bool
        """
        return self._call_driver("delete", key)

    def flush_all(self):
        """
        Flushes all stored values in backend server

        returns bool
        """
        return self._call_driver("flush_all")

    ##
    ## set operations
    ##
    def add(self, key, val, time=0, min_compress_len=0):
        """
        Sets a value to server identified by `key`, iff it does not
        already exist.

        Arguments:
          key -- string key
          val -- value to set

        Keyword arguments:
          time -- how far into the future to expire. default:0 (means never)
          min_compress_length -- minimum string size to attempt to compress.
                                 default:0 (0 means never compress)

        returns bool
        """
        return self._set("add", key, val, time, min_compress_len)

    def append(self, key, val, time=0, min_compress_len=0):
        """
        Appends `val` to stored data at `key`, iff `key` exists.

        Arguments:
          key -- string key
          val -- value to set

        Keyword arguments:
          time -- how far into the future to expire. default:0 (means never)
          min_compress_length -- minimum string size to attempt to compress.
                                 default:0 (0 means never compress)

        returns bool
        """
        return self._set("append", key, val, time, min_compress_len)

    def prepend(self, key, val, time=0, min_compress_len=0):
        """
        Prepends `val` to stored data at `key`, iff `key` exists.

        Arguments:
          key -- string key
          val -- value to set

        Keyword arguments:
          time -- how far into the future to expire. default:0 (means never)
          min_compress_length -- minimum string size to attempt to compress.
                                 default:0 (0 means never compress)

        returns bool
        """
        return self._set("prepend", key, val, time, min_compress_len)

    def replace(self, key, val, time=0, min_compress_len=0):
        """
        Replaces currently stored value at `key` with `val`, iff `key` exists.

        Arguments:
          key -- string key
          val -- value to set

        Keyword arguments:
          time -- how far into the future to expire. default:0 (means never)
          min_compress_length -- minimum string size to attempt to compress.
                                 default:0 (0 means never compress)

        returns bool
        """
        return self._set("replace", key, val, time, min_compress_len)

    def set(self, key, val, time=0, min_compress_len=0):
        """
        Sets stored value at `key` to `val`

        Arguments:
          key -- string key
          val -- value to set

        Keyword arguments:
          time -- how far into the future to expire. default:0 (means never)
          min_compress_length -- minimum string size to attempt to compress.
                                 default:0 (0 means never compress)

        returns bool
        """
        return self._set("set", key, val, time, min_compress_len)

    #def set_multi(self):
    #    ## unsupport!

    def cas(self, key, val, time=0, min_compress_len=0):
        """
        Check-and-Set sets stored value at `key` to `val`, iff it has not
        been modified since it was retrieved via `gets` or `gets_multi`.

        If `cache_cas=True` was not passed to init, then this effectively
        becomes a `set`.

        Arguments:
          key -- string key
          val -- value to set

        Keyword arguments:
          time -- how far into the future to expire. default:0 (means never)
          min_compress_length -- minimum string size to attempt to compress.
                                 default:0 (0 means never compress)

        returns bool
        """
        return self._set("cas", key, val, time, min_compress_len)

    ##
    ## get operations
    ##
    def get(self, key):
        """
        gets a stored value at `key`
        The stored value is unpacked.

        Arguments:
          key -- string key

        returns int or str or long or object or None
        """
        return self._get("get", key)

    def get_multi(self, keys):
        """
        gets a stored value for each key in `keys`
        The stored values are unpacked.

        Arguments:
          keys -- iterable of str

        returns dict -- dict key is matching key from `keys`.
                        dict[key] is int or str or long or object or None.
        """
        return self._get_multi('get_multi', keys)

    def gets(self, key):
        """
        gets a stored value at `key`, and caches the CAS_ID if
        `cache_cas=True` was passed to init.
        The stored value is unpacked.

        Arguments:
          key -- string key

        returns int or str or long or object or None
        """
        return self._get("gets", key)

    def gets_multi(self, keys):
        """
        gets a stored value for each key in `keys`, and caches the CAS_ID
        for each key if `cache_cas=True` was passed to init.
        The stored values are unpacked.

        Arguments:
          keys -- iterable of str

        returns dict -- dict key is matching key from `keys`.
                        dict[key] is int or str or long or object or None.
        """
        return self._get_multi('gets_multi', keys)

    ##
    ## data massaging methods
    ##
    def check_key(self, key):
        """
        Checks key for conformance, and encoding.

        Arguments:
          key -- string key

        returns string -- sanitized key
        raises MemcacheKeyError -- on bad key
        """
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
                val = pickle.dumps(val, self.pickle_proto)

        lv = len(val)
        #  do not store if value length exceeds maximum
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
        return self._call_driver(cmd, *args)

    def _get(self, cmd, key):
        key = self.check_key(key)
        response = self._call_driver(cmd, key)
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
        response = self._call_driver(cmd, keys)
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

    def _call_driver(self, cmd, *args):
        try:
            if not self._client:
                self.connect()
            return getattr(self._client, cmd)(*args)
        except socket.error as e:
            self.close()
            if self.error_as_miss:
                return None
            ## reraise wrapped, but with original exception included in args
            ## to provide for callers to introspect.
            raise MemcacheSocketException(str(e), e)
        except (RuntimeError, IOError) as e:
            self.close()
            if self.error_as_miss:
                return None
            ## reraise wrapped, but with original exception included in args
            ## to provide for callers to introspect.
            raise MemcacheDriverException(str(e), e)
