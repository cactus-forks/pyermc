# -*- coding: utf8 -*-

import os
import unittest
import pyermc
import inspect
import struct
import copy
import socket


MEMCACHED_HOST='127.0.0.1'
MEMCACHED_PORT=int(os.environ.get('MEMCACHED_TEST_PORT', 55555))
MEMCACHED_RUNNING = False

def memcached_running():
    global MEMCACHED_RUNNING
    if MEMCACHED_RUNNING:
        return True

    # hack to see if memcached is running
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((MEMCACHED_HOST, MEMCACHED_PORT))
        s.send("VERSION\r\n")
        data = s.recv(100)
        s.close()
        if data:
            MEMCACHED_RUNNING = True
            return True
    except:
        pass
    return False


class FooStruct(object):
    def __init__(self):
        self.bar = "baz"
    def __str__(self):
        return "A FooStruct"
    def __eq__(self, other):
        if isinstance(other, FooStruct):
            return self.bar == other.bar
        return False


@unittest.skipIf(
    memcached_running()!=True,
    "memcached not running: %s:%s" % (MEMCACHED_HOST, MEMCACHED_PORT))
class _IntegrationBase(unittest.TestCase):
    def do_setget(self, value, key=None, time=0, min_compress_len=0):
        if key is None:
            key = inspect.stack()[2][3]
        self.client.flush_all()
        self.client.set(key, value, time, min_compress_len)
        return self.client.get(key)

    def test_bad_driver(self):
        with self.assertRaisesRegexp(TypeError, 'Bad driver'):
            client = pyermc.Client(
                self.host, self.port, client_driver=None)
        with self.assertRaisesRegexp(TypeError, 'must be a class'):
            client = pyermc.Client(
                self.host, self.port, client_driver=43)
        with self.assertRaisesRegexp(TypeError, 'Bad driver provided'):
            client = pyermc.Client(
                self.host, self.port, client_driver=FooStruct)

    def test_string(self):
        val = 'test_setget'
        newval = self.do_setget(val)
        self.assertEqual(newval, val)

    def test_pickle(self):
        val = FooStruct()
        newval = self.do_setget(val)
        self.assertEqual(newval, val)
        self.assertEqual(str(newval), str(val))
        self.assertEqual(type(newval), type(val))

    def test_int(self):
        val = 1
        newval = self.do_setget(val)
        self.assertEqual(newval, val)

    def test_long(self):
        val = long(1<<30)
        newval = self.do_setget(val)
        self.assertEqual(newval, val)

    def test_unicode_value(self):
        val = u'über'
        newval = self.do_setget(val)
        self.assertEqual(newval, val)

    def test_unicode_key(self):
        key = u'üî™'
        val = u'über'
        newval = self.do_setget(val, key=key)
        self.assertEqual(newval, val)

    def test_control_chars(self):
        with self.assertRaises(pyermc.MemcacheKeyError):
            self.do_setget(1, key="this\x10has\x11control characters\x02")

    def test_long_key(self):
        key = 'a'*pyermc.MAX_KEY_LENGTH
        # this one is ok
        self.do_setget(1, key=key)

        # this one is too long
        key = key + 'a'
        with self.assertRaises(pyermc.MemcacheKeyError):
            self.do_setget(1, key=key)

        ## same with unicodes
        key = u'a'*pyermc.MAX_KEY_LENGTH
        self.do_setget(1, key=key)

        key = key + u'a'
        with self.assertRaises(pyermc.MemcacheKeyError):
            self.do_setget(1, key=key)

        # and no encoded
        key = u'a'*pyermc.MAX_KEY_LENGTH
        self.do_setget(1, key=key.encode('utf-8'))

        key = key + u'a'
        with self.assertRaises(pyermc.MemcacheKeyError):
            self.do_setget(1, key=key.encode('utf-8'))


    def test_long_value(self):
        val = 'a' * pyermc.MAX_VALUE_LENGTH
        self.do_setget(val)

        val = val + 'aaaaaa'
        with self.assertRaises(pyermc.MemcacheValueError):
            self.do_setget(val)

    def test_compression(self):
        val = 'a' * pyermc.MAX_VALUE_LENGTH
        self.do_setget(val, min_compress_len=1)

    def test_delete(self):
        key = 'test_delete'
        val = 'honk'
        newval = self.do_setget(val, key=key)
        self.assertEqual(val, newval)
        self.client.delete(key)
        newval = self.client.get(key)
        self.assertEqual(newval, None)

        ## test fail
        resp = self.client.delete(key + "junk")
        self.assertEqual(resp, False)

    def test_append(self):
        key = 'test_append'
        val = 'this '
        val2 = 'is a test'
        newval = self.do_setget(val, key=key)
        self.assertEqual(val, newval)
        self.client.append(key, val2)
        newval = self.client.get(key)
        self.assertEqual(val+val2, newval)

        val = self.client.append(key+"junk", val2)
        self.assertEqual(val, False)

    def test_prepend(self):
        key = 'test_prepend'
        val = 'this '
        val2 = 'is a test'
        newval = self.do_setget(val2, key=key)
        self.assertEqual(val2, newval)
        self.client.prepend(key, val)
        newval = self.client.get(key)
        self.assertEqual(val+val2, newval)

        val = self.client.append(key+"junk", val2)
        self.assertEqual(val, False)

    def test_incr(self):
        self.client.flush_all()
        key = 'test_incr'
        self.client.set(key, 1)
        # try incr by default of 1
        self.client.incr(key)
        # and by manually specified
        self.client.incr(key, 1)
        val = self.client.get(key)
        self.assertEqual(val, 3)

        # test incr a nonexistent value -- should result in None, because
        # key must exist first.
        key2 = key+"_2"
        self.client.incr(key2, 1)
        val = self.client.get(key2)
        self.assertEqual(val, None)

        # test incr a string
        key3 = key+"_3"
        self.client.set(key3, "1")
        self.client.incr(key3, 1)
        # type error to try incrementing by a string
        with self.assertRaises(TypeError):
            self.client.incr(key3, "1")
        val = self.client.get(key3)
        # oddly enough, you get the same type back out
        # as you start with, just after magick maths!
        self.assertEqual(val, "2")

        ### this seems like a bug in the underlying driver...
        key4 = key+"_4"
        self.client.set(key4, 2)
        ## note! if you increment by negative 3, you get wrapping!
        self.client.incr(key4, -1)
        val = self.client.get(key4)
        self.assertEqual(val, 1)

        ## test inc that doesn't exist
        resp = self.client.incr(key4+"junk", 1)
        self.assertEqual(resp, None)

    def test_decr(self):
        self.client.flush_all()
        key = 'test_decr'
        self.client.set(key, 5)
        # try decr by default of 1
        self.client.decr(key)
        # and by manually specified
        self.client.decr(key, 1)
        val = self.client.get(key)
        self.assertEqual(val, 3)

        # test decr a nonexistent value -- should result in None, because
        # key must exist first.
        key2 = key+"_2"
        self.client.decr(key2, 1)
        val = self.client.get(key2)
        self.assertEqual(val, None)

        # test decr a string
        key3 = key+"_3"
        self.client.set(key3, "2")
        self.client.decr(key3, 1)
        # type error to try decr by a string
        with self.assertRaises(TypeError):
            self.client.decr(key3, "1")
        val = self.client.get(key3)
        # oddly enough, you get the same type back out
        # as you start with, just after magick maths!
        self.assertEqual(val, "1")

        ### this seems like a bug in the underlying driver...
        key4 = key+"_4"
        self.client.set(key4, 2)
        self.client.decr(key4, -5)
        val = self.client.get(key4)
        self.assertEqual(val, 7)

        ## test inc that doesn't exist
        resp = self.client.decr(key4+"junk", 1)
        self.assertEqual(resp, None)

    def test_cas(self):
        self.client.flush_all()
        self.client.reset_client()
        self.client.cache_cas = True

        key = 'test_cas'
        val = 42
        # good cas pass
        self.client.set(key, val)
        v1 = self.client.gets(key)
        self.assertEqual(v1, val)
        self.client.cas(key, 47)
        v2 = self.client.get(key)
        self.assertEqual(v2, 47)

        # conflicting cas pass
        self.client.set(key, val)
        v1 = self.client.gets(key)
        self.assertEqual(v1, val)
        self.client.set(key, 44)
        # this will fail (returns None)
        self.client.cas(key, 47)
        v2 = self.client.get(key)
        self.assertEqual(v2, 44)

        ## test something not in cas_ids. should just be a set
        self.client.cas(key+"notinids", 123)
        v3 = self.client.get(key+"notinids")
        self.assertEqual(v3, 123)

        # turn it back off
        self.client.reset_client()
        self.client.cache_cas = False

    def test_get_multi(self):
        self.client.flush_all()
        data = {'test_get_multi_%s'%x:x for x in xrange(10)}

        for k,v in data.iteritems():
            self.client.set(k, v)
            z = self.client.get(k)
            self.assertEqual(v, z)

        data2 = self.client.get_multi(data.keys())
        self.assertDictEqual(data, data2)

        ## test some junk
        data = {'test_get_multi_%s_junk'%x:x for x in xrange(10)}
        data2 = self.client.get_multi(data.keys())
        self.assertDictEqual(data2, {})

    def test_gets_multi(self):
        self.client.flush_all()
        self.client.reset_client()
        self.client.cache_cas = True

        data = {'test_gets_multi_%s'%x:x for x in xrange(3)}

        for k,v in data.iteritems():
            self.client.set(k, v)
            z = self.client.get(k)
            self.assertEqual(v, z)

        data2 = self.client.gets_multi(data.keys())
        self.assertDictEqual(data, data2)
        data3 = {x:y+1 for x,y in data2.items()}
        for k,v in data3.iteritems():
            self.client.cas(k, v)
            z = self.client.get(k)
            self.assertEqual(v, z)
        data4 = self.client.get_multi(data3.keys())
        self.assertDictEqual(data3, data4)

        # turn it back off
        self.client.reset_client()
        self.client.cache_cas = False

    def test_get_logic(self):
        self.client.flush_all()
        key = 'test_get_logic'
        # and by manually specified
        self.client.set(key, 0)
        val = self.client.get(key)
        self.assertEqual(val, 0)

        self.client.set(key, 1)
        val = self.client.get(key)
        self.assertEqual(val, 1)

        self.client.set(key, long(1))
        val = self.client.get(key)
        self.assertEqual(val, long(1))

        self.client.set(key, "1")
        val = self.client.get(key)
        self.assertEqual(val, "1")

        self.client.set(key, False)
        val = self.client.get(key)
        self.assertEqual(val, False)

        self.client.set(key, True)
        val = self.client.get(key)
        self.assertEqual(val, True)

    def test_gets_logic(self):
        self.client.flush_all()
        key = 'test_gets_logic'
        # and by manually specified
        self.client.set(key, 0)
        val = self.client.gets(key)
        self.assertEqual(val, 0)

        self.client.set(key, 1)
        val = self.client.gets(key)
        self.assertEqual(val, 1)

        self.client.set(key, long(1))
        val = self.client.gets(key)
        self.assertEqual(val, long(1))

        self.client.set(key, "1")
        val = self.client.gets(key)
        self.assertEqual(val, "1")

        self.client.set(key, False)
        val = self.client.gets(key)
        self.assertEqual(val, False)

        self.client.set(key, True)
        val = self.client.gets(key)
        self.assertEqual(val, True)

    def test_empty_get(self):
        self.client.flush_all()
        key = 'test_empty_get'
        # and by manually specified
        val = self.client.get(key)
        self.assertEqual(val, None)

    def test_empty_gets(self):
        self.client.flush_all()
        key = 'test_empty_gets'
        # and by manually specified
        val = self.client.gets(key)
        self.assertEqual(val, None)

    def test_add(self):
        self.client.flush_all()
        key = 'test_add'
        val = self.client.add(key, 'test')
        self.assertEqual(val, True)
        val = self.client.add(key, 'test')
        self.assertEqual(val, False)

    def test_replace(self):
        self.client.flush_all()
        key = 'test_replace'
        val = self.client.replace(key, 'test')
        self.assertEqual(val, False)
        self.client.set(key, 'test')
        val = self.client.replace(key, 'test_new')
        self.assertEqual(val, True)
        val = self.client.get(key)
        self.assertEqual(val, 'test_new')

    def test_stats(self):
        v = self.client.stats()
        # test "truthiness".
        self.assertTrue(v)

    def test_version(self):
        v = self.client.version()
        # test "truthiness".
        self.assertTrue(v)

    def test_is_connected(self):
        v = self.client.is_connected()
        self.assertTrue(v)
        v = self.client.close()
        v = self.client.is_connected()
        self.assertFalse(v)

    def test_connect(self):
        v = self.client.close()
        v = self.client.is_connected()
        self.assertFalse(v)

        self.client.connect()
        v = self.client.is_connected()
        self.assertTrue(v)

        # test connect if connected already
        self.client.connect()
        v = self.client.is_connected()
        self.assertTrue(v)

        # test reconnect
        self.client.connect()
        # get a ref to the old class that wont change
        old_client = copy.copy(self.client._client)
        self.client.connect(reconnect=True)
        v = self.client.is_connected()
        self.assertTrue(v)
        self.assertNotEqual(old_client, self.client._client)

    def test_dont_raise_on_delete(self):
        # try to force an error
        self.client.close()
        # this is a bad value for this, using it to coarce del to try
        # to reclose when already closed
        self.client._client = True

        # this should not raise
        del self.client

    def test_badkeys(self):
        with self.assertRaisesRegexp(pyermc.MemcacheKeyError, 'None'):
            self.client.check_key(None)

        with self.assertRaisesRegexp(pyermc.MemcacheKeyError, 'Control'):
            self.client.check_key(u'\x00')

        with self.assertRaisesRegexp(pyermc.MemcacheKeyError, 'Control'):
            self.client.check_key("test\n")

        with self.assertRaisesRegexp(pyermc.MemcacheKeyError, 'must be a str'):
            self.client.check_key(1)

    def test_implements(self):
        t = (
            ('connect', (True,)),
            ('is_connected', tuple()),
            ('stats', tuple()),
            ('version', tuple()),
            ('flush_all', tuple()),
            ('delete', ("test_implements",)),
            ('set', ("test_implements", 1)),
            ('incr', ("test_implements",)),
            ('decr', ("test_implements",)),
            ('get', ("test_implements",)),
            ('gets', ("test_implements",)),
            ('cas', ("test_implements", 2)),
            ('get_multi', (["test_implements", "test_implements2"],)),
            ('gets_multi', (["test_implements", "test_implements2"],)),
            ('add', ("test_implements_a", "a")),
            ('append', ("test_implements", "a")),
            ('prepend', ("test_implements", "a")),
            ('replace', ("test_implements", "b")),
            ('close', tuple()),
        )
        for attr, params in t:
            a = getattr(self.client, attr, None)
            self.assertIsNotNone(a)
            a(*params)


class TestPyErMCNativeTextProto(_IntegrationBase):
    def setUp(self):
        self.host = MEMCACHED_HOST
        self.port = MEMCACHED_PORT
        from pyermc.driver.ultramemcache import UMemcacheDriver
        self.client = pyermc.Client(
            self.host, self.port, client_driver=UMemcacheDriver)
        self.client.connect()

class TestPyErMCNativeTextProto(_IntegrationBase):
    def setUp(self):
        self.host = MEMCACHED_HOST
        self.port = MEMCACHED_PORT
        from pyermc.driver.textproto import TextProtoDriver
        self.client = pyermc.Client(
            self.host, self.port, client_driver=TextProtoDriver)
        self.client.connect()

class TestPyErMCNativeBinaryProto(_IntegrationBase):
    def setUp(self):
        self.host = MEMCACHED_HOST
        self.port = MEMCACHED_PORT
        from pyermc.driver.binaryproto import BinaryProtoDriver
        self.client = pyermc.Client(
            self.host, self.port, client_driver=BinaryProtoDriver)
        self.client.connect()

    ## binary protocol behaves differently with regard to increment
    # These commands will either add or remove the specified amount to the
    # requested counter. If you want to set the value of the counter with
    # add/set/replace, the objects data must be the ascii representation of the
    # value and not the byte values of a 64 bit integer.
    #
    # If the counter does not exist, one of two things may happen:
    # * If the expiration value is all one-bits (0xffffffff), the operation
    #   will fail with NOT_FOUND.
    # * For all other expiration values, the operation will succeed by seeding
    #   the value for this key with the provided initial value to expire with
    #   the provided expiration time. The flags will be set to zero.
    #   Decrementing a counter will never result in a "negative value" (or
    #   cause the counter to "wrap"). instead the counter is set to 0.
    #   Incrementing the counter may cause the counter to wrap.
    def test_incr(self):
        self.client.flush_all()
        key = 'test_incr'
        self.client.set(key, 1)
        # try incr by default of 1
        self.client.incr(key)
        # and by manually specified
        self.client.incr(key, 1)
        val = self.client.get(key)
        self.assertEqual(val, 3)

        # test incr a nonexistent value -- should result in a default of 0.
        # note that this value will not make sense via GET, as it will contain
        # int 'as a string' (eg. '0\x00\x00\x00\x00\x00\x00\x00\x00...').
        # To test, do two incs in a row and see if 2 is returned, or intize
        # the response before evaling.
        # key must exist first.
        key2 = key+"_2"
        self.client.incr(key2, 1)
        # reliable way to get inc value back as int for 'default inc' binary
        # proto behavior
        v = self.client.incr(key2, 0)
        self.assertEqual(v, 0)
        self.client.incr(key2, 1)
        self.client.incr(key2, 1)
        v = self.client.incr(key2, 0)
        self.assertEqual(v, 2)

        # test incr a string
        key3 = key+"_3"
        self.client.set(key3, "1")
        self.client.incr(key3, 1)
        # type error to try incrementing by a string
        with self.assertRaises(TypeError):
            self.client.incr(key3, "1")
        val = self.client.get(key3)
        # oddly enough, you get the same type back out
        # as you start with, just after magick maths!
        self.assertEqual(val, "2")

        ### this seems like a bug in the underlying driver...
        key4 = key+"_4"
        self.client.set(key4, 2)
        ## note! if you increment by negative 3, you get wrapping!
        self.client.incr(key4, -1)
        val = self.client.get(key4)
        self.assertEqual(val, 1)

    def test_decr(self):
        self.client.flush_all()
        key = 'test_decr'
        self.client.set(key, 5)
        # try decr by default of 1
        self.client.decr(key)
        # and by manually specified
        self.client.decr(key, 1)
        val = self.client.get(key)
        self.assertEqual(val, 3)

        # test incr a nonexistent value -- should result in a default of 0.
        # note that this value will not make sense via GET, as it will contain
        # int 'as a string' (eg. '0\x00\x00\x00\x00\x00\x00\x00\x00...').
        # To test, do two incs in a row and see if 2 is returned, or intize
        # the response before evaling.
        # key must exist first.
        key2 = key+"_2"
        self.client.decr(key2, 1)
        val = self.client.incr(key2, 0)
        self.assertEqual(val, 0)
        self.client.decr(key2, 1)
        val = self.client.incr(key2, 0)
        self.assertEqual(val, 0)

        # test decr a string
        key3 = key+"_3"
        self.client.set(key3, "2")
        self.client.decr(key3, 1)
        # type error to try decr by a string
        with self.assertRaises(TypeError):
            self.client.decr(key3, "1")
        val = self.client.get(key3)
        # oddly enough, you get the same type back out
        # as you start with, just after magick maths!
        self.assertEqual(val, "1")

        ### this seems like a bug in the underlying driver...
        key4 = key+"_4"
        self.client.set(key4, 2)
        self.client.decr(key4, -5)
        val = self.client.get(key4)
        self.assertEqual(val, 7)
