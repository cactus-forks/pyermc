# -*- coding: utf8 -*-
"""
unit tests (mostly negative) for code paths that aren't covered by
the integration tests.
"""

import mock
import socket
import struct
import unittest
from mock import sentinel
from pyermc.driver import binaryproto, textproto
from pyermc.driver.base import Driver, TCPDriver
from pyermc.driver.binaryproto import BinaryProtoDriver
from pyermc.driver.textproto import TextProtoDriver


class TestDriver(unittest.TestCase):
    def test_not_implemented(self):
        """Should raise not implemented for all the methods in the base driver.
        """
        driver = Driver()
        calls = {'is_connected': [],
                 'connect': [],
                 'close': [],
                 'stats': [],
                 'version': [],
                 'flush_all': [],
                 'delete': ['foo'],
                 'incr': ['foo', 1],
                 'decr': ['foo', 1],
                 'cas': ['foo', 1, 2, 3, 4],
                 'get': ['foo'],
                 'gets': ['foo'],
                 'get_multi': [['foo', 'bar']],
                 'gets_multi': [['foo', 'bar']],
                 'add': ['foo', 1, 2, 3],
                 'append': ['foo', 1, 2, 3],
                 'prepend': ['foo', 1, 2, 3],
                 'replace': ['foo', 1, 2, 3],
                 'set': ['foo', 1, 2, 3]}
        for method_name, args in calls.iteritems():
            method = getattr(driver, method_name, None)
            self.assertIsNotNone(method, ("Driver should have method %s" %
                                          method_name))
            with self.assertRaises(NotImplementedError):
                method(*args)

        # test the socket property too
        with self.assertRaises(NotImplementedError):
            s = driver.socket


class TestTCPDriver(unittest.TestCase):
    @mock.patch('pyermc.driver.base.socket.socket')
    def test_connect_socket_error(self, mock_socket_constructor):
        """connect() should clear _sock and re-raise on socket.error.
        """
        err = socket.error('Mock error')
        mock_socket = mock.Mock()
        mock_socket.connect = mock.Mock(side_effect=err)
        mock_socket_constructor.return_value = mock_socket
        driver = TCPDriver('127.0.0.1', 55555, 1, 1)
        with self.assertRaisesRegexp(socket.error, 'Mock error'):
            driver.connect()
        self.assertIsNone(driver._sock)

    @mock.patch('pyermc.driver.base.socket.socket')
    def test_connect_socket_timeout(self, mock_socket_constructor):
        """connect() should clear _sock and re-raise on socket.timeout.
        """
        err = socket.timeout('Mock timeout')
        mock_socket = mock.Mock()
        mock_socket.connect = mock.Mock(side_effect=err)
        mock_socket_constructor.return_value = mock_socket
        driver = TCPDriver('127.0.0.1', 55555, 1, 1)
        with self.assertRaisesRegexp(socket.timeout, 'Mock timeout'):
            driver.connect()
        self.assertIsNone(driver._sock)

    def test_readbuffered_empty(self):
        """readbuffered() should close the socket and raise an error when
        recv() returns empty.
        """
        driver = TCPDriver('127.0.0.1', 55555, 1, 1)
        driver._sock = mock.Mock()
        driver._sock.recv = mock.Mock(return_value=None)
        driver.close = mock.Mock()
        with self.assertRaisesRegexp(socket.error, 'Socket died'):
            driver._readbuffered()
        driver.close.assert_called()

    def test_sendall_connect(self):
        """sendall() should connect if necessary
        """
        driver = TCPDriver('127.0.0.1', 55555, 1, 1)
        driver._sock = mock.Mock()
        driver.connect = mock.Mock()
        driver.is_connected = mock.Mock(return_value=False)
        driver._sendall(sentinel.data)
        driver.connect.assert_called()
        driver._sock.sendall.assert_called_with(sentinel.data)
        # should not connect if already connected
        driver._sock.reset_mock()
        driver.connect.reset_mock()
        driver.is_connected.return_value = True
        driver._sendall(sentinel.data)
        driver.connect.assert_not_called()
        driver._sock.sendall.assert_called_width(sentinel.data)

    def test_get_socket(self):
        """socket property should return underlying socket
        """
        driver = TCPDriver('127.0.0.1', 55555, 1, 1)
        driver._sock = sentinel.sock
        self.assertEqual(driver.socket, sentinel.sock)


class TestBinaryProtoDriver(unittest.TestCase):
    def test_read_response_bad_magic(self):
        """_read_response() should raise when magic is not the expected value.
        """
        bad_magic = 42
        header = struct.pack('!BBHBBHLLQ', bad_magic, 0, 0, 0, 0, 0, 0, 0, 0)
        driver = BinaryProtoDriver('127.0.0.1', 55555, 1, 1)
        driver._read = mock.Mock(return_value=header)
        with self.assertRaisesRegexp(IOError, 'Protocol violation'):
            driver._read_response()

    def test_incrdecr_bad_opcode(self):
        """_incrdecr() should raise when the opcode doesn't match cmd.
        """
        opcode = binaryproto.CMD_DECR
        status = binaryproto.RESPONSE_SUCCESS
        bodylen = 8
        driver = BinaryProtoDriver('127.0.0.1', 55555, 1, 1)
        driver._sendall = mock.Mock()
        driver._read_response = mock.Mock(return_value=[0, opcode, 0, 0, 0,
                                                        status, bodylen, 0, 0,
                                                        0, 0, 0])
        with self.assertRaisesRegexp(IOError, 'Unexpected response'):
            driver._incrdecr(binaryproto.CMD_INCR, 'foo', 1, 2)

    def test_incrdecr_bad_bodylen(self):
        """_incrdecr() should raise when the bodylen isn't 8.
        """
        opcode = binaryproto.CMD_INCR
        status = binaryproto.RESPONSE_SUCCESS
        bodylen = 0
        driver = BinaryProtoDriver('127.0.0.1', 55555, 1, 1)
        driver._sendall = mock.Mock()
        driver._read_response = mock.Mock(return_value=[0, opcode, 0, 0, 0,
                                                        status, bodylen, 0, 0,
                                                        0, 0, 0])
        with self.assertRaisesRegexp(IOError, 'Unexpected response size'):
            driver._incrdecr(binaryproto.CMD_INCR, 'foo', 1, 2)

    def test_incrdecr_bad_status(self):
        """_incrdecr() should return None when the status is not success.
        """
        opcode = binaryproto.CMD_INCR
        status = binaryproto.RESPONSE_UNKNOWN_COMMAND
        bodylen = 8
        driver = BinaryProtoDriver('127.0.0.1', 55555, 1, 1)
        driver._sendall = mock.Mock()
        driver._read_response = mock.Mock(return_value=[0, opcode, 0, 0, 0,
                                                        status, bodylen, 0, 0,
                                                        0, 0, 0])
        self.assertIsNone(driver._incrdecr(binaryproto.CMD_INCR, 'foo', 1, 2))

    def test_get_bad_opcode(self):
        """_get() should raise if opcode is not CMD_GETQ or CMD_GET.
        """
        opcode = binaryproto.CMD_SET
        driver = BinaryProtoDriver('127.0.0.1', 55555, 1, 1)
        driver._sendall = mock.Mock()
        driver._read_response = mock.Mock(return_value=[0, opcode, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0])
        with self.assertRaisesRegexp(IOError, 'Unexpected response'):
            driver._get([], None)

    def test_append_prepend_bad_opcode(self):
        """_append_prepend() should raise if opcode doesn't match cmd.
        """
        opcode = binaryproto.CMD_PREPEND
        driver = BinaryProtoDriver('127.0.0.1', 55555, 1, 1)
        driver._sendall = mock.Mock()
        driver._read_response = mock.Mock(return_value=[0, opcode, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0])
        with self.assertRaisesRegexp(IOError, 'Unexpected response'):
            driver._append_prepend(binaryproto.CMD_APPEND, 'foo', 'bar', 1, 2)

    def test_set_bad_opcode(self):
        """_set() should raise if opcode doesn't match cmd.
        """
        opcode = binaryproto.CMD_APPEND
        driver = BinaryProtoDriver('127.0.0.1', 55555, 1, 1)
        driver._sendall = mock.Mock()
        driver._read_response = mock.Mock(return_value=[0, opcode, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0])
        with self.assertRaisesRegexp(IOError, 'Unexpected response'):
            driver._set(binaryproto.CMD_SET, 'foo', 'bar', 1, 2)

    def test_stats_bad_opcode(self):
        """stats() should raise if opcode is not CMD_STAT.
        """
        opcode = binaryproto.CMD_GET
        driver = BinaryProtoDriver('127.0.0.1', 55555, 1, 1)
        driver._sendall = mock.Mock()
        driver._read_response = mock.Mock(return_value=[0, opcode, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0])
        with self.assertRaisesRegexp(IOError, 'Unexpected response'):
            driver.stats()

    def test_version_bad_opcode(self):
        """version() should raise if opcode is not CMD_VERSION.
        """
        opcode = binaryproto.CMD_GET
        driver = BinaryProtoDriver('127.0.0.1', 55555, 1, 1)
        driver._sendall = mock.Mock()
        driver._read_response = mock.Mock(return_value=[0, opcode, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0])
        with self.assertRaisesRegexp(IOError, 'Unexpected response'):
            driver.version()

    def test_flush_all_bad_opcode(self):
        """flush_all() should raise if opcode is not CMD_FLUSH.
        """
        opcode = binaryproto.CMD_GET
        driver = BinaryProtoDriver('127.0.0.1', 55555, 1, 1)
        driver._sendall = mock.Mock()
        driver._read_response = mock.Mock(return_value=[0, opcode, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0])
        with self.assertRaisesRegexp(IOError, 'Unexpected response'):
            driver.flush_all()

    def test_flush_all_bad_status(self):
        """flush_all() should return False if status is not success.
        """
        opcode = binaryproto.CMD_FLUSH
        status = binaryproto.RESPONSE_UNKNOWN_COMMAND
        driver = BinaryProtoDriver('127.0.0.1', 55555, 1, 1)
        driver._sendall = mock.Mock()
        driver._read_response = mock.Mock(return_value=[0, opcode, 0, 0, 0,
                                                        status, 0, 0, 0, 0,
                                                        0, 0])
        self.assertFalse(driver.flush_all())

    def test_delete_bad_opcode(self):
        """delete() should raise if opcode is not CMD_DELETE.
        """
        opcode = binaryproto.CMD_GET
        driver = BinaryProtoDriver('127.0.0.1', 55555, 1, 1)
        driver._sendall = mock.Mock()
        driver._read_response = mock.Mock(return_value=[0, opcode, 0, 0, 0,
                                                        0, 0, 0, 0, 0, 0, 0])
        with self.assertRaisesRegexp(IOError, 'Unexpected response'):
            driver.delete('foo')


class TestTextProtoDriver(unittest.TestCase):
    def test_read_data_response_error(self):
        """_read_data_response() should raise on an error.
        """
        driver = TextProtoDriver('127.0.0.1', 55555, 1, 1)
        driver._readline = mock.Mock(return_value=textproto.ERROR)
        with self.assertRaisesRegexp(
                IOError, 'NonExistent command sent to server'):
            driver._read_data_response()

    def test_read_data_response_client_error(self):
        """_read_data_response() should raise on a client error.
        """
        line = '%s ohai' % (textproto.CLIENT_ERROR,)
        driver = TextProtoDriver('127.0.0.1', 55555, 1, 1)
        driver._readline = mock.Mock(return_value=line)
        with self.assertRaisesRegexp(IOError, 'ohai'):
            driver._read_data_response()

    def test_read_data_response_server_error(self):
        """_read_data_response() should raise on a server error.
        """
        line = '%s ohai' % (textproto.SERVER_ERROR,)
        driver = TextProtoDriver('127.0.0.1', 55555, 1, 1)
        driver._readline = mock.Mock(return_value=line)
        with self.assertRaisesRegexp(IOError, 'ohai'):
            driver._read_data_response()
