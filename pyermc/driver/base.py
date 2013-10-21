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

import sys
import errno
import socket


class Driver(object):
    def is_connected(self):
        """
        answer as to whether the client is connected

        returns: bool
        """
        raise NotImplementedError

    def connect(self, reconnect=False):
        """
        should connect to the underlying socket
        may perform needed conn setup (eg clear buffers, etc).

        param: reconnect
               if true, means the driver should disconnect/reconnect if already
               connected.

        returns: None
        """
        raise NotImplementedError

    @property
    def socket(self):
        """
        should return the underlying socket, if present.
        None if not present (driver specific).

        returns: socket.socket or None
        """
        raise NotImplementedError

    def close(self):
        """
        disconnect

        returns: None
        """
        raise NotImplementedError

    def stats(self):
        """
        performs STATS

        returns: dict
                 stats key/value pairs
        """
        raise NotImplementedError

    def version(self):
        """
        performs VERSION

        returns: str
                 version string
        """
        raise NotImplementedError

    def flush_all(self):
        """
        performs FLUSH_ALL

        returns: bool
                 success or failure of command
        """
        raise NotImplementedError

    def delete(self, key):
        """
        performs DELETE <key>

        returns: bool
                 success or failure of command
        """
        raise NotImplementedError

    def incr(self, key, val):
        """
        performs INCR <key>

        returns: None or int
                 If increment fails, return None
                 If increment succeeds, return the new value after increment
        """
        raise NotImplementedError

    def decr(self, key, val):
        """
        performs DECR <key>

        returns: None or int
                 If decrement fails, return None
                 If decrement succeeds, return the new value after decrement
        """
        raise NotImplementedError

    def cas(self, key, val, cas_id, time, flags):
        """
        performs CAS <key> <value> <cas>

        returns: bool
                 True/False success or failure of command
        """
        raise NotImplementedError

    def get(self, key):
        """
        performs GET <key>

        returns: None or list
                 If get fails, return None
                 If get succeeds, return a list of:
                    [str, int]
                    element 0 is the data response as str. element 1 is the
                    flag bitfield as int.
        """
        raise NotImplementedError

    def gets(self, key):
        """
        performs GETS <key>

        returns: None or list
                 If get fails, return None
                 If get succeeds, return a list of:
                    [str, int, int]
                    element 0 is the data response as str. element 1 is the
                    flag bitfield as int. element 2 is the CAS_ID.
        """
        raise NotImplementedError

    def get_multi(self, keys):
        """
        performs GET_MULTI <key1> <key2> ...

        returns: dict
                 A dictionary of key/val where the key was present in ``keys``
                 param AND exists on the server. The value is a list of:
                    [str, int]
                    element 0 is the data response as str. element 1 is the
                    flag bitfield as int.
                 dictionary may be empty if no keys were present on the server
        """
        raise NotImplementedError

    def gets_multi(self, keys):
        """
        performs GET_MULTI <key1> <key2> ...

        returns: dict
                 A dictionary of key/val where the key was present in ``keys``
                 param AND exists on the server. The value is a list of:
                    [str, int, int]
                    element 0 is the data response as str. element 1 is the
                    flag bitfield as int. element 2 is the CAS_ID.
                 dictionary may be empty if no keys were present on the server
        """
        raise NotImplementedError

    def add(self, key, val, time, flags):
        """
        performs ADD <key> <value>

        returns: bool
                 True/False success or failure of command
        """
        raise NotImplementedError

    def append(self, key, val, time, flags):
        """
        performs APPEND <key> <value>

        returns: bool
                 True/False success or failure of command
        """
        raise NotImplementedError

    def prepend(self, key, val, time, flags):
        """
        performs PREPEND <key> <value>

        returns: bool
                 True/False success or failure of command
        """
        raise NotImplementedError

    def replace(self, key, val, time, flags):
        """
        performs REPLACE <key> <value>

        returns: bool
                 True/False success or failure of command
        """
        raise NotImplementedError

    def set(self, key, val, time, flags):
        """
        performs SET <key> <value>

        returns: bool
                 True/False success or failure of command
        """
        raise NotImplementedError


class TCPDriver(Driver):
    def __init__(self, host, port, timeout, connect_timeout,
                 disable_nagle=True):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.disable_nagle = disable_nagle
        self._buffer = ''
        self._sock = None

    ###
    ### connection handling
    def connect(self, reconnect=False):
        if self.is_connected():
            if not reconnect:
                return
            self.close()

        self._buffer = ''
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(self.connect_timeout)
        self._sock.settimeout(self.timeout)
        try:
            self._sock.connect((self.host, self.port))
            self._sock.settimeout(self.timeout)
        except (socket.error, socket.timeout):
            self._sock = None  # don't want to hang on to bad socket
            raise
        # set socket for tcp keepalive
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if self.disable_nagle:
            # disable nagle, as memcache deals with lots of small packets.
            self._sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)

    def is_connected(self):
        if not self._sock:
            return False
        return True
        ## this is arguably safer, but it slows things down a
        ## non-insignificant amount. Consider putting this into pooling
        ## code instead of per-memcache call, and overriding?
        #try:
        #    self._sock.settimeout(0)
        #    if sys.platform != 'linux':
        #        # recv(0) on darwin doesn't work like it does on linux, so
        #        # on darwin use recv(1, MSG_PEEK) to hopefully acheive
        #        # something similar. although this code path should work on
        #        # linux, retain linux specific behavior as originally coded
        #        # for now, so as to reduce changeset in prod.
        #        self._sock.recv(1, socket.MSG_PEEK)
        #    else:
        #        # linux works fine with recv(0)
        #        self._sock.recv(0)
        #    # if recv didn't raise, then the socket was closed or there
        #    # is junk in the read buffer, either way, close
        #    self.close()
        #except socket.error as e:
        #    # this is expected if the socket is still open
        #    if e.errno == errno.EAGAIN:
        #        self._sock.settimeout(self.timeout)
        #        return True
        #    else:
        #        self.close()
        #return False

    @property
    def socket(self):
        return self._sock

    def close(self):
        if self._sock:
            self._sock.close()
            self._sock = None

    def _readbuffered(self):
        data = self._sock.recv(4096)
        if not data:
            # conn closed? abort
            self.close()
            raise socket.error('Socket died')
        self._buffer += data

    def _read(self, size):
        while len(self._buffer) < size:
            self._readbuffered()

        b = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return b

    def _sendall(self, data):
        if not self.is_connected():
            self.connect()
        self._sock.sendall(data)

    # alias for convenience
    _send = _sendall
