# -*- coding: utf8 -*-

import unittest
import pyermc
import inspect


class FooStruct(object):
    def __init__(self):
        self.bar = "baz"
    def __str__(self):
        return "A FooStruct"
    def __eq__(self, other):
        if isinstance(other, FooStruct):
            return self.bar == other.bar
        return False


class TestPyErMC(unittest.TestCase):

    def setUp(self):
        self.host = "127.0.0.1"
        self.port = 11211
        self.client = pyermc.Client(self.host, self.port)
        self.client.connect()

    def do_setget(self, value, key=None, time=0, min_compress_len=0):
        if key is None:
            key = inspect.stack()[2][3]
        self.client.flush_all()
        self.client.set(key, value, time, min_compress_len)
        return self.client.get(key)

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
        key = 'a'*pyermc.SERVER_MAX_KEY_LENGTH
        # this one is ok
        self.do_setget(1, key=key)

        # this one is too long
        key = key + 'a'
        with self.assertRaises(pyermc.MemcacheKeyError):
            self.do_setget(1, key=key)

        ## same with unicodes
        key = u'a'*pyermc.SERVER_MAX_KEY_LENGTH
        self.do_setget(1, key=key)

        key = key + u'a'
        with self.assertRaises(pyermc.MemcacheKeyError):
            self.do_setget(1, key=key)

        # and no encoded
        key = u'a'*pyermc.SERVER_MAX_KEY_LENGTH
        self.do_setget(1, key=key.encode('utf-8'))

        key = key + u'a'
        with self.assertRaises(pyermc.MemcacheKeyError):
            self.do_setget(1, key=key.encode('utf-8'))


    def test_long_value(self):
        val = 'a' * pyermc.SERVER_MAX_VALUE_LENGTH
        self.do_setget(val)

        val = val + 'aaaaaa'
        with self.assertRaises(pyermc.MemcacheValueError):
            self.do_setget(val)

    def test_compression(self):
        val = 'a' * pyermc.SERVER_MAX_VALUE_LENGTH
        self.do_setget(val, min_compress_len=1)

        # compressed. should fit
        val = val + 'aaaaaa'
        self.do_setget(val, min_compress_len=1)

    def test_delete(self):
        key = 'test_delete'
        val = 'honk'
        newval = self.do_setget(val, key=key)
        self.assertEqual(val, newval)
        self.client.delete(key)
        newval = self.client.get(key)
        self.assertEqual(newval, None)

    def test_append(self):
        key = 'test_append'
        val = 'this '
        val2 = 'is a test'
        newval = self.do_setget(val, key=key)
        self.assertEqual(val, newval)
        self.client.append(key, val2)
        newval = self.client.get(key)
        self.assertEqual(val+val2, newval)

    def test_prepend(self):
        key = 'test_prepend'
        val = 'this '
        val2 = 'is a test'
        newval = self.do_setget(val2, key=key)
        self.assertEqual(val2, newval)
        self.client.prepend(key, val)
        newval = self.client.get(key)
        self.assertEqual(val+val2, newval)

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
        self.assertEqual(val, 0)

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
            print self.client.cas(k, v)
            z = self.client.get(k)
            self.assertEqual(v, z)
        data4 = self.client.get_multi(data3.keys())
        self.assertDictEqual(data3, data4)

        # turn it back off
        self.client.reset_client()
        self.client.cache_cas = False


    ### need tests for:
    ###     get_multi
    ###     gets_multi
    ### memcache not connected, error states, etc....
