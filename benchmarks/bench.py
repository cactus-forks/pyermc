import pyermc
from pyermc.driver.ultramemcache import UMemcacheDriver
from pyermc.driver.textproto import TextProtoDriver
from pyermc.driver.binaryproto import BinaryProtoDriver
import memcache
import time
import timeit
import sys
from contextlib import contextmanager

MEMCACHED_HOST='127.0.0.1'
MEMCACHED_PORT=55555
ITERATIONS = 5000

LONG_KEY = "a"*200
LONG_STR = "wutwutwutwutwut" * 1024

class TestObject(object):
    def __init__(self):
        self.thing = 'i am a thing!'
        self.thing_string = (
            'i have a loooooooooooooooooooooooooooooooooooooooo'
            'oooooooooooooooooooooooooooooooooooooooooooooooooo'
            'oooooooooooooooooooooooooooooooooooooooooooooooooo'
            'oooooooooooooooooooooooooooooooooooooooooooooooooo'
            'oooooooooooooooooooooooooooooooooooooooooooooooooo'
            'oooooooooooooooooooooooooooooooooooooooooooooooooo'
            'oooooooooooooooooooooooooooooooooooooooooooooooooo'
            'oooooooooooooooooooooooooooooooooooooongish string'
            )
        self.i = 100

## build conns
pyermc_conn = pyermc.Client(
    MEMCACHED_HOST, MEMCACHED_PORT, client_driver=UMemcacheDriver)
pyermc_textproto_conn = pyermc.Client(
    MEMCACHED_HOST, MEMCACHED_PORT, client_driver=TextProtoDriver)
pyermc_binaryproto_conn = pyermc.Client(
    MEMCACHED_HOST, MEMCACHED_PORT, client_driver=BinaryProtoDriver)
memcache_conn = memcache.Client(
    ['%s:%s' % (MEMCACHED_HOST, MEMCACHED_PORT)], pickleProtocol=2)
# connect
pyermc_conn.connect()
pyermc_textproto_conn.connect()
pyermc_binaryproto_conn.connect()

# build pickle'able object
test_o = TestObject()

def do_bench(name, test_method, conn_name):
    t = timeit.timeit(
            "%s('%s',conn)" % (test_method,conn_name),
            setup="from __main__ import %s, %s as conn" % (
                test_method, conn_name), number=ITERATIONS)
    print("%s: %s" % (name, t))

def test_validate_key(name, conn):
    if conn == pyermc_conn:
        conn.check_key("s"*100)
    if conn == memcache_conn:
        conn.check_key("s"*100)

def test_set_long_key(name, conn):
    conn.set("%s:%s" % (name, LONG_KEY), "some value")

def test_set_string(name, conn):
    conn.set("%s:key_1" % name, "some value")

def test_set_int(name, conn):
    conn.set("%s:key_2" % name, 1)

def test_set_long(name, conn):
    conn.set("%s:key_3" % name, long(1))

def test_set_pickle(name, conn):
    conn.set("%s:key_4" % name, test_o)

def test_set_longerstring(name, conn):
    conn.set("%s:key_5" % name, LONG_STR)

def test_set_longstring_compress(name, conn):
    conn.set(
        "%s:key_6" % name,
        "wutwutwutwutwut" * 60000,
        min_compress_len=100)

def test_get_long_key(name, conn):
    conn.get("%s:%s" % (name, "a"*200))

def test_get_string(name, conn):
    conn.get("%s:key_1" % name)

def test_get_int(name, conn):
    conn.get("%s:key_2" % name)

def test_get_long(name, conn):
    conn.get("%s:key_3" % name)

def test_get_pickle(name, conn):
    conn.get("%s:key_4" % name)

def test_get_longerstring(name, conn):
    conn.get("%s:key_5" % name)

def test_get_longstring_compress(name, conn):
    conn.get("%s:key_6" % name)

## not a real multi-set; just gets memcached ready for getmulti
def prep_test_getmulti(name, conn):
    keys = (
        "%s:mkey_1" % name,
        "%s:mkey_2" % name,
        "%s:mkey_3" % name,
        "%s:mkey_4" % name,
        "%s:mkey_5" % name,
        "%s:mkey_6" % name,
        )
    for k in keys:
        conn.set(k, 'for multi-get')

def test_getmulti(name, conn):
    keys = (
        "%s:mkey_1" % name,
        "%s:mkey_2" % name,
        "%s:mkey_3" % name,
        "%s:mkey_4" % name,
        "%s:mkey_5" % name,
        "%s:mkey_6" % name,
        )
    conn.get_multi(keys)

def test_add(name, conn):
    conn.add('%s:onlyonce' % name, 1)

def test_replace(name, conn):
    conn.replace('%s:onlyonce' % name, 1)

def test_incr(name, conn):
    conn.incr('%s:counter' % name, 1)

def test_decr(name, conn):
    conn.decr('%s:counter' % name, 1)

def test_append(name, conn):
    conn.append('%s:append' % name, 'a')

def test_prepend(name, conn):
    conn.append('%s:prepend' % name, 'a')

def test_delete(name, conn):
    conn.delete("%s:key_1" % name)
    conn.delete("%s:key_2" % name)
    conn.delete("%s:key_3" % name)
    conn.delete("%s:key_4" % name)
    conn.delete("%s:key_5" % name)
    conn.delete("%s:key_6" % name)

# make sure we have connected drivers first, and do test prep
prep_test_getmulti("test_setmulti", pyermc_conn)
prep_test_getmulti("test_setmulti", pyermc_textproto_conn)
prep_test_getmulti("test_setmulti", pyermc_binaryproto_conn)
prep_test_getmulti("test_setmulti", memcache_conn)

# now do the measurement
pyermc_wins = 0
pyermc_losses = 0
for method in (
    "test_validate_key",
    "test_set_long_key",
    "test_set_string",
    "test_set_int",
    "test_set_long",
    "test_set_pickle",
    "test_set_longerstring",
    "test_set_longstring_compress",
    "test_get_long_key",
    "test_get_string",
    "test_get_int",
    "test_get_long",
    "test_get_pickle",
    "test_get_longerstring",
    "test_get_longstring_compress",
    "test_getmulti",
    "test_add",
    "test_replace",
    "test_incr",
    "test_decr",
    "test_append",
    "test_prepend",
    "test_delete"
    ):
    do_bench("pyermc.umc %s" % method, method, "pyermc_conn")
    do_bench("pyermc.txp %s" % method, method, "pyermc_textproto_conn")
    do_bench("pyermc.bxp %s" % method, method, "pyermc_binaryproto_conn")
    do_bench("memcache   %s" % method, method, "memcache_conn")
    print ""
