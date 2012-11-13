# -*- coding: utf8 -*-

import lz4
import pickle
import unittest
import mock
from mock import sentinel
from pyermc import memcache

class TestClient(unittest.TestCase):
    def test_init(self):
        client = memcache.Client('1.2.3.4', 5678, connect_timeout=11,
                                 timeout=22, server_max_key_length=33,
                                 server_max_value_length=44, pickle=False,
                                 cache_cas=True, debug=True)
        self.assertEqual(client.host, '1.2.3.4')
        self.assertEqual(client.port, 5678)
        self.assertEqual(client.connect_timeout, 11)
        self.assertEqual(client.timeout, 22)
        self.assertEqual(client.server_max_key_length, 33)
        self.assertEqual(client.server_max_value_length, 44)
        self.assertFalse(client.pickle)
        self.assertTrue(client.cache_cas)
        self.assertTrue(client.debug)
        self.assertIsNone(client._client)
        self.assertFalse(client._connected)
        self.assertEqual({}, client.cas_ids)

    def test_init_defaults(self):
        client = memcache.Client('1.2.3.4', 5678)
        self.assertEqual(client.host, '1.2.3.4')
        self.assertEqual(client.port, 5678)
        self.assertEqual(client.timeout, 3)
        self.assertEqual(client.connect_timeout, 3)
        self.assertEqual(client.server_max_key_length,
                         memcache.SERVER_MAX_KEY_LENGTH)
        self.assertEqual(client.server_max_value_length,
                         memcache.SERVER_MAX_VALUE_LENGTH)
        self.assertTrue(client.pickle)
        self.assertFalse(client.cache_cas)
        self.assertFalse(client.debug)
        self.assertIsNone(client._client)
        self.assertFalse(client._connected)
        self.assertEqual({}, client.cas_ids)

    def test_flags(self):
        self.assertEqual(memcache.Client._FLAG_PICKLE, 1<<0)
        self.assertEqual(memcache.Client._FLAG_INTEGER, 1<<1)
        self.assertEqual(memcache.Client._FLAG_LONG, 1<<2)
        self.assertEqual(memcache.Client._FLAG_COMPRESSED, 1<<3)

    def test_connect(self):
        client = memcache.Client('127.0.0.1', 11211,
                                 connect_timeout=sentinel.connect_timeout,
                                 timeout=sentinel.timeout)
        mock_client = mock.Mock()
        with mock.patch('umemcache.Client') as MockClient:
            MockClient.return_value = mock_client
            client.connect()
            MockClient.assert_called_with('127.0.0.1:11211')
        mock_client.sock.settimeout.assert_has_calls(
            [mock.call(sentinel.connect_timeout),
             mock.call(sentinel.timeout)])
        self.assertIs(client._client, mock_client)
        self.assertTrue(client._connected)

    def test_connect_connected(self):
        """connect() should not reconnect if already connected, by default.
        """
        client = memcache.Client('127.0.0.1', 11211)
        client._client = sentinel._client
        client._connected = True
        client.connect()
        self.assertIs(client._client, sentinel._client)
        self.assertTrue(client._connected)

    def test_connect_reconnect(self):
        """connect() should reconnect if already connected and reconnect=True.
        """
        mock_old_client = mock.Mock()
        client = memcache.Client('127.0.0.1', 11211,
                                 connect_timeout=sentinel.connect_timeout,
                                 timeout=sentinel.timeout)
        client._client = mock_old_client
        client._connected = True
        mock_new_client = mock.Mock()
        with mock.patch('umemcache.Client') as MockClient:
            MockClient.return_value = mock_new_client
            client.connect(reconnect=True)
            MockClient.assert_called_with('127.0.0.1:11211')
        self.assertIs(client._client, mock_new_client)
        mock_old_client.close.assert_called_with()
        mock_new_client.connect.assert_called_with()
        mock_new_client.sock.settimeout.assert_has_calls(
            [mock.call(sentinel.connect_timeout),
             mock.call(sentinel.timeout)])
        self.assertTrue(client._connected)

    def test_close(self):
        mock_client = mock.Mock()
        client = memcache.Client('127.0.0.1', 11211)
        client._client = mock_client
        client._connected = True
        client.close()
        self.assertIsNone(client._client)
        self.assertFalse(client._connected)
        mock_client.close.assert_called_with()

    def test_is_connected(self):
        client = memcache.Client('127.0.0.1', 11211)
        client._connected = True
        client._client = mock.Mock()
        client._client.is_connected = mock.Mock(return_value=True)
        self.assertIsNone(client.is_connected())

    def test_is_connected_client_is_not_connected(self):
        """is_connected() should return False if _client is not connected.
        """
        client = memcache.Client('127.0.0.1', 11211)
        client._connected = True
        client._client = mock.Mock()
        client._client.is_connected = mock.Mock(return_value=False)
        self.assertFalse(client.is_connected())

    def test_is_connected_client_is_none(self):
        """is_connected() should return False if _client is None.
        """
        client = memcache.Client('127.0.0.1', 11211)
        client._connected = True
        self.assertFalse(client.is_connected())

    def test_is_connected_connected_is_false(self):
        """is_connected() should return false if _connected is False.
        """
        client = memcache.Client('127.0.0.1', 11211)
        client._connected = False
        client._client = mock.Mock()
        client._client.is_connected = mock.Mock(return_value=True)
        self.assertFalse(client.is_connected())
        client._client.is_connected.assert_has_calls([])

    def test_disconnect(self):
        """disconnect() should be an alias for close().
        """
        self.assertEqual(memcache.Client.disconnect, memcache.Client.close)

    def test_reset_client(self):
        client = memcache.Client('127.0.0.1', 11211)
        with mock.patch.object(client, 'reset_cas') as mock_reset_cas:
            client.reset_client()
            mock_reset_cas.assert_called_with()

    def test_reset_cas(self):
        client = memcache.Client('127.0.0.1', 11211)
        client.cas_ids = sentinel._cas_ids
        client.reset_cas()
        self.assertEqual(client.cas_ids, {})

    def test_check_key(self):
        client = memcache.Client('127.0.0.1', 11211, server_max_key_length=1)
        self.assertEqual(client.check_key('f'), 'f')
        with self.assertRaisesRegexp(memcache.MemcacheKeyError, 'length'):
            client.check_key('ff')
        with self.assertRaisesRegexp(memcache.MemcacheKeyError, 'length'):
            client.check_key('f', key_extra_len=1)

    def test_check_key_tuple(self):
        """check_key() should use the second item of key, if key is a tuple.
        """
        client = memcache.Client('127.0.0.1', 11211)
        mock_key = mock.Mock(spec=tuple)
        mock_key.__getitem__ = mock.Mock(return_value='foo')
        self.assertEqual(client.check_key(mock_key), 'foo')
        mock_key.__getitem__.assert_called_with(1)

    def test_check_key_empty(self):
        """check_key() should raise when the key is empty.
        """
        client = memcache.Client('127.0.0.1', 11211)
        for empty_key in [False, None, 0, '']:
            with self.assertRaisesRegexp(memcache.MemcacheKeyError, 'None'):
                client.check_key(empty_key)

    def test_check_key_invalid_utf8(self):
        """check_key() should raise if the key is unicode and not valid UTF-8.
        """
        client = memcache.Client('127.0.0.1', 11211)
        mock_key = mock.Mock(spec=unicode)
        mock_key.encode = mock.Mock(side_effect=Exception)
        with self.assertRaisesRegexp(memcache.MemcacheKeyError, 'unicode'):
            client.check_key(mock_key)

    def test_check_key_not_a_string(self):
        """check_key() should raise if the key is not a string.
        """
        client = memcache.Client('127.0.0.1', 11211)
        with self.assertRaisesRegexp(memcache.MemcacheKeyError, 'str'):
            client.check_key(object())

    def test_check_key_invalid_character(self):
        """check_key() should raise if the key contains control characters.
        """
        client = memcache.Client('127.0.0.1', 11211)
        for code in range(33) + [127]:
            with self.assertRaisesRegexp(memcache.MemcacheKeyError, 'Control'):
                client.check_key(chr(code))

    def test_stats(self):
        """stats() should pass through to _client.stats()
        """
        client = memcache.Client('127.0.0.1', 11211)
        client._client = mock.Mock()
        client._client.stats.return_value = sentinel.stats_result
        self.assertIs(client.stats(), sentinel.stats_result)
        client._client.stats.assert_called_with()

    def test_version(self):
        """version() should pass through to _client.version()
        """
        client = memcache.Client('127.0.0.1', 11211)
        client._client = mock.Mock()
        client._client.version.return_value = sentinel.version_result
        self.assertIs(client.version(), sentinel.version_result)
        client._client.version.assert_called_with()

    def test_decr(self):
        """decr() should pass through to _client.decr()
        """
        client = memcache.Client('127.0.0.1', 11211)
        client._client = mock.Mock()
        client._client.decr.return_value = sentinel.decr_result
        self.assertIs(client.decr('foo', 1), sentinel.decr_result)
        client._client.decr.assert_called_with('foo', 1)

    def test_incr(self):
        """incr() should pass through to _client.incr()
        """
        client = memcache.Client('127.0.0.1', 11211)
        client._client = mock.Mock()
        client._client.incr.return_value = sentinel.incr_result
        self.assertIs(client.incr('foo', 1), sentinel.incr_result)
        client._client.incr.assert_called_with('foo', 1)

    def test_delete(self):
        """delete() should pass through to _client.delete()
        """
        client = memcache.Client('127.0.0.1', 11211)
        client._client = mock.Mock()
        client._client.delete.return_value = sentinel.delete_result
        self.assertIs(client.delete('foo'), sentinel.delete_result)
        client._client.delete.assert_called_with('foo')

    def test_flush_all(self):
        """flush_all() should pass through to _client.flush_all()
        """
        client = memcache.Client('127.0.0.1', 11211)
        client._client = mock.Mock()
        client._client.flush_all.return_value = sentinel.flush_all_result
        self.assertIs(client.flush_all(), sentinel.flush_all_result)
        client._client.flush_all.assert_called_with()

    def test_add(self):
        client = memcache.Client('127.0.0.1', 11211)
        with mock.patch.object(client, '_set') as mock_set:
            client.add('foo', 1)
            client.add('foo', 1, time=2, min_compress_len=3)
            mock_set.assert_has_calls([mock.call('add', 'foo', 1, 0, 0),
                                       mock.call('add', 'foo', 1, 2, 3)])

    def test_append(self):
        client = memcache.Client('127.0.0.1', 11211)
        with mock.patch.object(client, '_set') as mock_set:
            client.append('foo', 1)
            client.append('foo', 1, time=2, min_compress_len=3)
            mock_set.assert_has_calls([mock.call('append', 'foo', 1, 0, 0),
                                       mock.call('append', 'foo', 1, 2, 3)])

    def test_prepend(self):
        client = memcache.Client('127.0.0.1', 11211)
        with mock.patch.object(client, '_set') as mock_set:
            client.prepend('foo', 1)
            client.prepend('foo', 1, time=2, min_compress_len=3)
            mock_set.assert_has_calls([mock.call('prepend', 'foo', 1, 0, 0),
                                       mock.call('prepend', 'foo', 1, 2, 3)])

    def test_replace(self):
        client = memcache.Client('127.0.0.1', 11211)
        with mock.patch.object(client, '_set') as mock_set:
            client.replace('foo', 1)
            client.replace('foo', 1, time=2, min_compress_len=3)
            mock_set.assert_has_calls([mock.call('replace', 'foo', 1, 0, 0),
                                       mock.call('replace', 'foo', 1, 2, 3)])

    def test_set(self):
        client = memcache.Client('127.0.0.1', 11211)
        with mock.patch.object(client, '_set') as mock_set:
            client.set('foo', 1)
            client.set('foo', 1, time=2, min_compress_len=3)
            mock_set.assert_has_calls([mock.call('set', 'foo', 1, 0, 0),
                                       mock.call('set', 'foo', 1, 2, 3)])

    def test_cas(self):
        client = memcache.Client('127.0.0.1', 11211)
        with mock.patch.object(client, '_set') as mock_set:
            client.cas('foo', 1)
            client.cas('foo', 1, time=2, min_compress_len=3)
            mock_set.assert_has_calls([mock.call('cas', 'foo', 1, 0, 0),
                                       mock.call('cas', 'foo', 1, 2, 3)])

    def test_val_to_store_info(self):
        client = memcache.Client('127.0.0.1', 11211)
        result = client._val_to_store_info('value', 0)
        self.assertEqual(result, (0, 5, 'value'))

    def test_val_to_store_info_int(self):
        """_val_to_store_info() should convert int values to strings.
        """
        client = memcache.Client('127.0.0.1', 11211)
        result = client._val_to_store_info(1337, 0)
        self.assertEqual(result, (memcache.Client._FLAG_INTEGER, 4, '1337'))

    def test_val_to_store_info_long(self):
        """_val_to_store_info() should convert long values to strings.
        """
        client = memcache.Client('127.0.0.1', 11211)
        result = client._val_to_store_info(1337L, 0)
        self.assertEqual(result, (memcache.Client._FLAG_LONG, 4, '1337'))

    def test_val_to_store_info_pickle(self):
        """_val_to_store_info() should pickle value if not a str, int or long.
        """
        value = {'foo': 1, 'bar': 2}
        pickled_value = pickle.dumps(value)
        client = memcache.Client('127.0.0.1', 11211)
        result = client._val_to_store_info(value, len(pickled_value))
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], memcache.Client._FLAG_PICKLE)
        self.assertEqual(result[1], len(pickled_value))
        self.assertEqual(pickle.loads(result[2]), value)

    def test_val_to_store_info_compress(self):
        """_val_to_store_info() should compress large values.
        """
        value = 'foo' * 32
        compressed_value = lz4.compress(value)
        client = memcache.Client('127.0.0.1', 11211)
        result = client._val_to_store_info(value, min_compress_len=1)
        self.assertEqual(result, (memcache.Client._FLAG_COMPRESSED,
                                  len(compressed_value), compressed_value))

    def test_value_to_store_info_compress_length(self):
        """ _val_to_store_info() should not use compressed values if too long.

        That is, if the compressed value is longer than the original value, use
        the original value instead.
        """
        value = '...'
        compressed_value = lz4.compress(value)
        self.assertGreater(len(compressed_value), len(value))
        client = memcache.Client('127.0.0.1', 11211)
        result = client._val_to_store_info(value, min_compress_len=1)
        self.assertEqual(result, (0, len(value), value))

    def test_val_to_store_info_compress_int(self):
        """_val_to_store_info() should not compress int values.
        """
        client = memcache.Client('127.0.0.1', 11211)
        result = client._val_to_store_info(1337, min_compress_len=1)
        self.assertEqual(result, (memcache.Client._FLAG_INTEGER, 4, '1337'))

    def test_val_to_store_info_compress_long(self):
        """_val_to_store_info() should not compress long values.
        """
        client = memcache.Client('127.0.0.1', 11211)
        result = client._val_to_store_info(1337L, min_compress_len=1)
        self.assertEqual(result, (memcache.Client._FLAG_LONG, 4, '1337'))

    def test_val_to_store_info_length(self):
        client = memcache.Client('127.0.0.1', 11211, server_max_value_length=1)
        with self.assertRaises(memcache.MemcacheValueError):
            client._val_to_store_info('foo', 0)

    def test_private_set(self):
        client = memcache.Client('127.0.0.1', 11211)
        mock_client = mock.Mock()
        mock_client.some_cmd.return_value = sentinel.some_cmd_return_value
        mock_check_key = mock.Mock(side_effect=lambda key: key)
        mock_connect = mock.Mock()
        mock_is_connected = mock.Mock(return_value=False)
        mock_val_to_store_info = mock.Mock(return_value=(sentinel.flags,
                                                         sentinel.sval_len,
                                                         sentinel.sval))
        with mock.patch.multiple(client, _client=mock_client,
                                 _val_to_store_info=mock_val_to_store_info,
                                 check_key=mock_check_key,
                                 connect=mock_connect,
                                 is_connected=mock_is_connected):
            result = client._set('some_cmd', 'some_key', 'some_value', 1, 2)
            mock_check_key.assert_called_with('some_key')
            mock_is_connected.assert_called_with()
            mock_connect.assert_called_with()
            mock_val_to_store_info.assert_called_with('some_value', 2)
            mock_client.some_cmd.assert_called_with('some_key', sentinel.sval,
                                                    1, sentinel.flags)
            self.assertIs(result, sentinel.some_cmd_return_value)

    def test_private_set_cas(self):
        mock_client = mock.Mock()
        mock_client.cas.return_value = sentinel.cas_result
        client = memcache.Client('127.0.0.1', 11211)
        client._client = mock_client
        client.cas_ids = {'key': sentinel.cas_id}
        self.assertIs(client._set('cas', 'key', 'val'), sentinel.cas_result)
        mock_client.cas.assert_called_with('key', 'val', sentinel.cas_id, 0, 0)
        mock_client.set.assert_has_calls([])

    def test_private_set_cas_no_cas_id(self):
        """_set() should call set instead of cas if key is not in cas_ids.
        """
        mock_client = mock.Mock()
        mock_client.set.return_value = sentinel.set_result
        client = memcache.Client('127.0.0.1', 11211)
        client._client = mock_client
        self.assertIs(client._set('cas', 'key', 'val'), sentinel.set_result)
        mock_client.set.assert_called_with('key', 'val', 0, 0)
        mock_client.cas.assert_has_calls([])

    def test_get(self):
        client = memcache.Client('127.0.0.1', 11211)
        with mock.patch.object(client, '_get') as mock_get:
            client.get('some_key')
            mock_get.assert_called_with('get', 'some_key')

    def test_gets(self):
        client = memcache.Client('127.0.0.1', 11211)
        with mock.patch.object(client, '_get') as mock_get:
            client.gets('some_key')
            mock_get.assert_called_with('gets', 'some_key')

    def test_get_multi(self):
        client = memcache.Client('127.0.0.1', 11211)
        with mock.patch.object(client, '_get_multi') as mock_get_multi:
            client.get_multi(['foo', 'bar'])
            mock_get_multi.assert_called_with('get_multi', ['foo', 'bar'])

    def test_gets_multi(self):
        client = memcache.Client('127.0.0.1', 11211)
        with mock.patch.object(client, '_get_multi') as mock_get_multi:
            client.gets_multi(['foo', 'bar'])
            mock_get_multi.assert_called_with('gets_multi', ['foo', 'bar'])

    @mock.patch('lz4.decompress')
    @mock.patch('cPickle.loads')
    def test_recv_value(self, mock_loads, mock_decompress):
        mock_decompress.return_value = sentinel.decompress_return_value
        mock_loads.return_value = sentinel.loads_return_value
        client = memcache.Client('127.0.0.1', 11211)
        # should return buf directly if no flags
        buf = 'x'
        self.assertIs(client._recv_value(buf, 0), buf)
        mock_decompress.assert_has_calls([])
        # should decompress the value
        self.assertIs(client._recv_value(buf, memcache.Client._FLAG_COMPRESSED),
                      sentinel.decompress_return_value)
        mock_decompress.assert_called_with(buf)
        # should cast to int
        result = client._recv_value('123', memcache.Client._FLAG_INTEGER)
        self.assertEqual(result, 123)
        self.assertIsInstance(result, int)
        with self.assertRaises(ValueError):
            client._recv_value('x', memcache.Client._FLAG_INTEGER)
        # should cast to long
        result = client._recv_value('123', memcache.Client._FLAG_LONG)
        self.assertEqual(result, 123)
        self.assertIsInstance(result, long)
        with self.assertRaises(ValueError):
            client._recv_value('x', memcache.Client._FLAG_LONG)
        # should unpickle
        self.assertIs(client._recv_value('x', memcache.Client._FLAG_PICKLE),
                      sentinel.loads_return_value)
        mock_loads.assert_called_with('x')

    def test_private_get(self):
        client = memcache.Client('127.0.0.1', 11211)
        mock_client = mock.Mock()
        mock_client.some_cmd.return_value = (sentinel.value, sentinel.flags)
        mock_check_key = mock.Mock(return_value='some_key')
        mock_connect = mock.Mock()
        mock_is_connected = mock.Mock(return_value=False)
        mock_recv_value = mock.Mock(return_value=sentinel.received_value)
        patches = {'_client': mock_client, '_recv_value': mock_recv_value,
                   'check_key': mock_check_key, 'connect': mock_connect,
                   'is_connected': mock_is_connected}
        with mock.patch.multiple(client, **patches):
            result = client._get('some_cmd', 'some_key')
            mock_check_key.assert_called_with('some_key')
            mock_is_connected.assert_called_with()
            mock_connect.assert_called_with()
            mock_client.some_cmd.assert_called_with('some_key')
            mock_recv_value.assert_called_with(sentinel.value, sentinel.flags)
            self.assertIs(result, sentinel.received_value)

    def test_private_get_empty_response(self):
        """_get() should always return None for falsey responses.
        """
        client = memcache.Client('127.0.0.1', 11211)
        for falsey_value in [None, False, 0, '']:
            mock_client = mock.Mock()
            mock_client.foo.return_value = falsey_value
            client._client = mock_client
            result = client._get('foo', 'bar')
            self.assertIsNone(result)
            mock_client.foo.assert_called_with('bar')

    def test_private_get_empty_response_val(self):
        """_get() should always return None for falsey response values.
        """
        client = memcache.Client('127.0.0.1', 11211)
        for falsey_value in [None, False, 0, '']:
            mock_client = mock.Mock()
            mock_client.foo.return_value = (falsey_value, 0)
            client._client = mock_client
            result = client._get('foo', 'bar')
            self.assertIsNone(result)
            mock_client.foo.assert_called_with('bar')

    def test_private_get_gets(self):
        """_get() should work properly for gets commands.
        """
        client = memcache.Client('127.0.0.1', 11211, cache_cas=True)
        with mock.patch.object(client, '_recv_value') as mock_recv_value:
            mock_recv_value.return_value = sentinel.received_value
            mock_client = mock.Mock()
            mock_client.gets.return_value = (sentinel.val, sentinel.flags,
                                             sentinel.cas_id)
            client._client = mock_client
            result = client._get('gets', 'key')
            self.assertIs(result, sentinel.received_value)
            self.assertEqual(client.cas_ids, {'key': sentinel.cas_id})

    def test_private_get_gets_cache_cas_false(self):
        """_get() should not cache cas ids if cache_cas is False.
        """
        client = memcache.Client('127.0.0.1', 11211, cache_cas=False)
        with mock.patch.object(client, '_recv_value') as mock_recv_value:
            mock_recv_value.return_value = sentinel.received_value
            mock_client = mock.Mock()
            mock_client.gets.return_value = (sentinel.val, sentinel.flags,
                                             sentinel.cas_id)
            client._client = mock_client
            result = client._get('gets', 'key')
            self.assertIs(result, sentinel.received_value)
            self.assertEqual(client.cas_ids, {})

    def test_private_get_multi(self):
        mock_client = mock.Mock()
        mock_client.cmd.return_value = {
            'key1': (sentinel.value1, sentinel.flags1),
            'key2': (sentinel.value2, sentinel.flags2)}
        mock_check_key = mock.Mock(side_effect=lambda key: key)
        mock_connect = mock.Mock()
        mock_is_connected = mock.Mock(return_value=False)
        recv_value_results = {sentinel.value1: sentinel.received_value1,
                              sentinel.value2: sentinel.received_value2}
        def recv_value_side_effect(value, flags):
            return recv_value_results[value]
        mock_recv_value = mock.Mock(side_effect=recv_value_side_effect)
        client = memcache.Client('127.0.0.1', 11211)
        patches = {'_client': mock_client, '_recv_value': mock_recv_value,
                   'check_key': mock_check_key, 'connect': mock_connect,
                   'is_connected': mock_is_connected}
        with mock.patch.multiple(client, **patches):
            result = client._get_multi('cmd', ['key1', 'key2', 'key3'])
            self.assertEqual(result, {'key1': sentinel.received_value1,
                                      'key2': sentinel.received_value2})
            mock_check_key.assert_has_calls([mock.call('key1'),
                                             mock.call('key2'),
                                             mock.call('key3')])
            mock_is_connected.assert_called_with()
            mock_connect.assert_called_with()
            mock_client.cmd.assert_called_with(['key1', 'key2', 'key3'])
            mock_recv_value.assert_any_call(sentinel.value1, sentinel.flags1)
            mock_recv_value.assert_any_call(sentinel.value2, sentinel.flags2)

    def test_private_get_multi_gets_multi(self):
        """_get_multi() should work properly for gets_multi commands.
        """
        client = memcache.Client('127.0.0.1', 11211, cache_cas=True)
        with mock.patch.object(client, '_recv_value') as mock_recv_value:
            recv_value_results = {sentinel.value1: sentinel.received_value1,
                                  sentinel.value2: sentinel.received_value2}
            def recv_value_side_effect(value, flags):
                return recv_value_results[value]
            mock_recv_value.side_effect = recv_value_side_effect
            mock_client = mock.Mock()
            mock_client.gets_multi.return_value = {
                'key1': (sentinel.value1, sentinel.flags1, sentinel.cas_id1),
                'key2': (sentinel.value2, sentinel.flags2, sentinel.cas_id2)}
            client._client = mock_client
            result = client._get_multi('gets_multi', ['key1', 'key2', 'key3'])
            self.assertEqual(result, {'key1': sentinel.received_value1,
                                      'key2': sentinel.received_value2})
            self.assertEqual(client.cas_ids, {'key1': sentinel.cas_id1,
                                              'key2': sentinel.cas_id2})

    def test_private_get_multi_gets_multi_cache_cas_false(self):
        """_get_multi() should not cache cas_ids if cache_cas is False.
        """
        client = memcache.Client('127.0.0.1', 11211, cache_cas=False)
        with mock.patch.object(client, '_recv_value') as mock_recv_value:
            recv_value_results = {sentinel.value1: sentinel.received_value1,
                                  sentinel.value2: sentinel.received_value2}
            def recv_value_side_effect(value, flags):
                return recv_value_results[value]
            mock_recv_value.side_effect = recv_value_side_effect
            mock_client = mock.Mock()
            mock_client.gets_multi.return_value = {
                'key1': (sentinel.value1, sentinel.flags1, sentinel.cas_id1),
                'key2': (sentinel.value2, sentinel.flags2, sentinel.cas_id2)}
            client._client = mock_client
            result = client._get_multi('gets_multi', ['key1', 'key2', 'key3'])
            self.assertEqual(result, {'key1': sentinel.received_value1,
                                      'key2': sentinel.received_value2})
            self.assertEqual(client.cas_ids, {})
