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
Memcache text protocol backend
"""

from .base import TCPDriver

OK           = 'OK'
END          = 'END'
STAT         = 'STAT'
VALUE        = 'VALUE'
ERROR        = 'ERROR'
VERSION      = 'VERSION'
STORED       = 'STORED'
DELETED      = 'DELETED'
NOT_FOUND    = 'NOT_FOUND'
CLIENT_ERROR = 'CLIENT_ERROR'
SERVER_ERROR = 'SERVER_ERROR'


class TextProtoDriver(TCPDriver):
    ###
    ### data readers
    def _readline(self):
        while True:
            index = self._buffer.find('\r\n')
            if index >= 0:
                break
            self._readbuffered()
        b = self._buffer[:index]
        self._buffer = self._buffer[index+2:]
        return b

    def _read_errors(self, line):
        parts = line.split()
        if parts[0] == ERROR:
            raise IOError('NonExistent command sent to server')
        elif parts[0] == CLIENT_ERROR:
            raise IOError(parts[1])
        elif parts[0] == SERVER_ERROR:
            raise IOError(parts[1])
        return

    def _read_data_response(self, cas=False):
        resp = ''
        values = {}
        while resp != END:
            resp = self._readline()
            parts = resp.split()
            if parts[0] == VALUE:
                lp = len(parts)
                if (lp == 4 and not cas) or (lp == 5 and cas):
                    bytes_to_read = int(parts[3]) + 2  # 2 extra for \r\n
                    data = self._read(bytes_to_read)[:-2]
                    values[parts[1]] = [data, int(parts[2])]
                    if cas:
                        values[parts[1]].append(int(parts[4]))
            elif parts[0] == STAT:
                values[parts[1]] = parts[2]
            elif parts[0] == VERSION:
                return parts[1]
            elif parts[0] == ERROR:
                self._read_errors(resp)
            elif parts[0] == CLIENT_ERROR:
                self._read_errors(resp)
            elif parts[0] == SERVER_ERROR:
                self._read_errors(resp)
        return values

    def _read_expect_response(self, exp=None):
        resp = self._readline()
        if resp == exp:
            return True
        self._read_errors(resp)
        return False

    ###
    ### helpful internal abstractions
    def _sendall(self, data):
        ## textproto requires a \r\n trailer
        super(TextProtoDriver, self)._sendall(data+'\r\n')

    def _incrdecr(self, cmd, key, val):
        fullcmd = "%s %s %s" % (cmd, key, val)
        self._sendall(fullcmd)
        resp = self._readline()
        if resp == NOT_FOUND:
            return
        self._read_errors(resp)
        return int(resp)

    def _get(self, cmd, key, cas):
        fullcmd = "%s %s" % (cmd, key)
        self._sendall(fullcmd)
        resp = self._read_data_response(cas=cas)
        return resp

    def _set(self, cmd, key, val, time, flags):
        fullcmd = "%s %s %d %d %d\r\n%s" % (
            cmd, key, flags, time, len(val), val)
        self._sendall(fullcmd)
        return self._read_expect_response(STORED)

    ###
    ### exposed driver methods
    def stats(self):
        self._sendall('stats')
        return self._read_data_response()

    def version(self):
        self._sendall('version')
        return self._read_data_response()

    def flush_all(self):
        self._sendall('flush_all')
        return self._read_expect_response(OK)

    def delete(self, key):
        fullcmd = "delete %s" % key
        self._sendall(fullcmd)
        return self._read_expect_response(DELETED)

    def incr(self, key, val):
        return self._incrdecr('incr', key, val)

    def decr(self, key, val):
        return self._incrdecr('decr', key, val)

    def cas(self, key, val, cas_id, time, flags):
        fullcmd = "cas %s %d %d %d %d\r\n%s" % (
            key, flags, time, len(val), cas_id, val)
        self._sendall(fullcmd)
        return self._read_expect_response(STORED)

    def get(self, key):
        resp = self._get('get', key, cas=False)
        if resp:
            return resp.popitem()[1]
        return None

    def gets(self, key):
        resp = self._get('gets', key, cas=True)
        if resp:
            return resp.popitem()[1]
        return None

    def get_multi(self, keys):
        return self._get('get', ' '.join(keys), cas=False)

    def gets_multi(self, keys):
        return self._get('gets', ' '.join(keys), cas=True)

    def add(self, key, val, time, flags):
        return self._set('add', key, val, time, flags)

    def append(self, key, val, time, flags):
        return self._set('append', key, val, time, flags)

    def prepend(self, key, val, time, flags):
        return self._set('prepend', key, val, time, flags)

    def replace(self, key, val, time, flags):
        return self._set('replace', key, val, time, flags)

    def set(self, key, val, time, flags):
        return self._set('set', key, val, time, flags)
