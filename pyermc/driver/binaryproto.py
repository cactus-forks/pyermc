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
Memcache binary protocol backend
"""

from .base import TCPDriver
import struct

## refs:
# http://code.google.com/p/memcached/wiki/BinaryProtocolRevamped
# https://github.com/memcached/memcached/blob/master/protocol_binary.h

# magic byte opcodes
MAGIC_REQUEST           = 0x80
MAGIC_RESPONSE          = 0x81

# response status opcodes
RESPONSE_SUCCESS         = 0x00
RESPONSE_KEY_ENOENT      = 0x01
RESPONSE_KEY_EEXISTS     = 0x02
RESPONSE_E2BIG           = 0x03
RESPONSE_EINVAL          = 0x04
RESPONSE_NOT_STORED      = 0x05
RESPONSE_DELTA_BADVAL    = 0x06
RESPONSE_AUTH_ERROR      = 0x20
RESPONSE_AUTH_CONTINUE   = 0x21
RESPONSE_UNKNOWN_COMMAND = 0x81
RESPONSE_ENOMEM          = 0x82

# command opcodes
CMD_GET             = 0x00
CMD_SET             = 0x01
CMD_ADD             = 0x02
CMD_REPLACE         = 0x03
CMD_DELETE          = 0x04
CMD_INCR            = 0x05
CMD_DECR            = 0x06
CMD_QUIT            = 0x07
CMD_FLUSH           = 0x08

CMD_GETQ            = 0x09
CMD_NOOP            = 0x0A
CMD_VERSION         = 0x0B
CMD_GETK            = 0x0c
CMD_GETKQ           = 0x0d
CMD_APPEND          = 0x0e
CMD_PREPEND         = 0x0f
CMD_STAT            = 0x10

## unused in this driver
#CMD_SETQ            = 0x11
#CMD_ADDQ            = 0x12
#CMD_REPLACEQ        = 0x13
#CMD_DELETEQ         = 0x14
#CMD_INCREMENTQ      = 0x15
#CMD_DECREMENTQ      = 0x16
#CMD_QUITQ           = 0x17
#CMD_FLUSHQ          = 0x18
#CMD_APPENDQ         = 0x19
#CMD_PREPENDQ        = 0x1a
#CMD_TOUCH           = 0x1c
#CMD_GAT             = 0x1d
#CMD_GATQ            = 0x1e
#CMD_GATK            = 0x23
#CMD_GATKQ           = 0x24
#
#CMD_SASL_LIST_MECHS = 0x20
#CMD_SASL_AUTH       = 0x21
#CMD_SASL_STEP       = 0x22

# data type opcodes
DATA_RAW             = 0x00


class BinaryProtoDriver(TCPDriver):
    ###
    ### data readers
    def _read_response(self):
        header = self._read(24)
        (magic, opcode, keylen, extlen, datatype,
         status, bodylen, opaque, cas) = struct.unpack('!BBHBBHLLQ', header)

        if magic != MAGIC_RESPONSE:
            raise IOError("Protocol violation")

        extra = None
        if extlen:
            extra = self._read(extlen)

        rkey = None
        if keylen:
            rkey = self._read(keylen)

        rval = None
        if bodylen:
            vallen = bodylen - extlen - keylen
            if vallen > 0:
                rval = self._read(vallen)

        return (magic, opcode, keylen, extlen, datatype,
                status, bodylen, opaque, cas, extra, rkey, rval)

    ###
    ### helpful internal abstractions
    def _build_request(self, cmd, header_extra=None, opaque=0, cas=0,
                       key=None, value=None):
        extralen = 0
        if header_extra:
            extralen = len(header_extra)

        keylen = 0
        if key:
            keylen = len(key)
            packed_key = struct.pack('%ds' % keylen, key)

        vallen = 0
        if value:
            vallen = len(value)
            packed_value = struct.pack('%ds' % vallen, value)

        header = [MAGIC_REQUEST, cmd, keylen, extralen, DATA_RAW,
                  0, keylen+extralen+vallen, opaque, cas]
        packed = struct.pack('!BBHBBHLLQ', *header)

        if extralen:
            packed += header_extra
        if keylen:
            packed += packed_key
        if vallen:
            packed += packed_value
        return packed

    def _incrdecr(self, cmd, key, val, time):
        req = self._build_request(
            cmd, key=key,
            header_extra=struct.pack('!QQL', val, 0, time))
        self._sendall(req)

        (magic, opcode, keylen, extlen, datatype, status,
         bodylen, opaque, cas, extra, rkey, rval) = self._read_response()

        if opcode != cmd:
            raise IOError('Unexpected response')

        if bodylen != 8:
            raise IOError('Unexpected response size!')

        if status != RESPONSE_SUCCESS:
            return None

        recval = struct.unpack('!Q', rval)[0]

        # if recval is 0, then that means it didn't incr/decr.
        # it could also mean that a decrement resulted in zero. not sure
        # if you can tell the difference....
        return int(recval)

    def _get(self, keys, cas):
        fullreq = ''
        counter = len(keys)
        while counter > 0:
            counter -= 1
            cmd = CMD_GETQ
            if counter == 0:
                cmd = CMD_GET
            ## note: use opaque field to tell which response maps to which
            ## key...
            key = keys[counter]
            req = self._build_request(cmd, opaque=counter, key=key)
            fullreq += req

        self._sendall(fullreq)
        results = {}

        while True:
            (magic, opcode, keylen, extlen, datatype, status,
             bodylen, opaque, cas_id, extra, rkey, rval
             ) = self._read_response()

            if opcode not in (CMD_GETQ, CMD_GET):
                raise IOError('Unexpected response')

            if status == RESPONSE_SUCCESS:
                rvallen = bodylen - keylen - extlen
                flags, val = struct.unpack('!L%ds' % rvallen, extra+rval)
                results[keys[opaque]] = [val, flags]
                if cas:
                    results[keys[opaque]].append(cas_id)
            if opaque == 0:
                break
        return results

    def _append_prepend(self, cmd, key, val, time, flags):
        req = self._build_request(cmd, key=key, value=val)
        self._sendall(req)

        (magic, opcode, keylen, extlen, datatype, status,
         bodylen, opaque, cas, extra, rkey, rval) = self._read_response()

        if opcode != cmd:
            raise IOError('Unexpected response')
        if status == RESPONSE_SUCCESS:
            return True
        return False

    def _set(self, cmd, key, val, time, flags, cas=0):
        req = self._build_request(
            cmd, cas=cas, key=key, value=str(val),
            header_extra=struct.pack('!LL', flags, time))
        self._sendall(req)

        (magic, opcode, keylen, extlen,
         datatype, status, bodylen, opaque, cas,
         extra, rkey, rval) = self._read_response()

        if opcode != cmd:
            raise IOError('Unexpected response')
        if status == RESPONSE_SUCCESS:
            return True
        return False

    ###
    ### exposed driver methods
    def stats(self):
        req = self._build_request(CMD_STAT)
        self._sendall(req)

        results = {}
        while True:
            (magic, opcode, keylen, extlen, datatype, status,
             bodylen, opaque, cas, extra, rkey, rval) = self._read_response()

            if opcode != CMD_STAT:
                raise IOError('Unexpected response')
            if keylen == 0:  # got the magic 'stats done' packet.
                break

            rvallen = bodylen - keylen - extlen
            key, val = struct.unpack('%ds%ds' % (keylen,rvallen), rkey+rval)
            results[key] = val
        return results

    def version(self):
        req = self._build_request(CMD_VERSION)
        self._sendall(req)

        (magic, opcode, keylen, extlen, datatype, status,
         bodylen, opaque, cas, extra, rkey, rval) = self._read_response()

        if opcode != CMD_VERSION:
            raise IOError('Unexpected response')

        rvallen = bodylen - keylen - extlen
        val = struct.unpack('%ds' % rvallen, rval)[0]
        return val

    def flush_all(self):
        ## flush has an optional header_extra for sending a 'flush in future'
        ## ... just support flushing immediately
        #header_extra=struct.pack('!L', expiry),
        req = self._build_request(CMD_FLUSH)
        self._sendall(req)

        (magic, opcode, keylen, extlen, datatype, status,
         bodylen, opaque, cas, extra, rkey, rval) = self._read_response()

        if opcode != CMD_FLUSH:
            raise IOError('Unexpected response')

        if status == RESPONSE_SUCCESS:
            return True
        return False

    def delete(self, key):
        req = self._build_request(CMD_DELETE, key=key)
        self._sendall(req)

        (magic, opcode, keylen, extlen, datatype, status,
         bodylen, opaque, cas, extra, rkey, rval) = self._read_response()

        if opcode != CMD_DELETE:
            raise IOError('Unexpected response')

        ## not sure how useful this data point is, so just ignore it..
        #if bodylen:
        #    ## an attempt to delete a non-existent key returns
        #    ## some garbage "Not Found" in the response.
        #    ## ಠ_ಠ
        #    rvallen = bodylen - keylen - extlen
        #    val = struct.unpack('%ds' % rvallen, rval)[0]
        if status == RESPONSE_SUCCESS:
            return True
        return False

    def incr(self, key, val, time=0):
        return self._incrdecr(CMD_INCR, key, val, time)

    def decr(self, key, val, time=0):
        return self._incrdecr(CMD_DECR, key, val, time)

    def get(self, key):
        keys = (key,)
        resp = self._get(keys, cas=False)
        if resp:
            return resp.popitem()[1]
        return None

    def gets(self, key):
        keys = (key,)
        resp = self._get(keys, cas=True)
        if resp:
            return resp.popitem()[1]
        return None

    def get_multi(self, keys):
        return self._get(keys, cas=False)

    def gets_multi(self, keys):
        return self._get(keys, cas=True)

    def append(self, key, val, time, flags):
        return self._append_prepend(CMD_APPEND, key, val, time, flags)

    def prepend(self, key, val, time, flags):
        return self._append_prepend(CMD_PREPEND, key, val, time, flags)

    def add(self, key, val, time, flags):
        return self._set(CMD_ADD, key, val, time, flags)

    def replace(self, key, val, time, flags):
        return self._set(CMD_REPLACE, key, val, time, flags)

    def cas(self, key, val, cas_id, time, flags):
        return self._set(CMD_SET, key, val, time, flags, cas_id)

    def set(self, key, val, time, flags):
        return self._set(CMD_SET, key, val, time, flags)
