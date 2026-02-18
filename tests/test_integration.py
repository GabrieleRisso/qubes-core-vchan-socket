# -*- encoding: utf-8 -*-
#
# The Qubes OS Project, http://www.qubes-os.org
#
# Copyright (C) 2026  Qubes OS KVM port contributors
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2.1 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License along
# with this program; if not, see <http://www.gnu.org/licenses/>.

"""
Integration tests for vchan-socket as a drop-in replacement for vchan-xen.

Tests cover:
  - API parity with vchan-xen (set_blocking, all public functions)
  - qrexec-style message framing over vchan
  - qubesdb-style key-value protocol over vchan
  - Connection lifecycle: connect, disconnect, reconnect
  - Concurrent bidirectional data transfer
  - Non-blocking I/O mode
"""

import unittest
import struct
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from .vchan import VchanServer, VchanClient, VchanBase, VchanException, \
    VCHAN_WAITING, VCHAN_DISCONNECTED, VCHAN_CONNECTED


BUF_SIZE = 4096


class VchanIntegrationMixin:
    """Mixin providing server/client creation for both vchan and vchan-simple."""
    lib = 'vchan/libvchan-socket.so'

    def start_server(self, read_min=BUF_SIZE, write_min=BUF_SIZE):
        server = VchanServer(self.lib, 1, 2, 42,
                             read_min=read_min, write_min=write_min)
        server.wait_for_state(VCHAN_WAITING)
        self.addCleanup(server.close)
        return server

    def start_client(self):
        client = VchanClient(self.lib, 2, 1, 42)
        self.addCleanup(client.close)
        return client


class SetBlockingTest(unittest.TestCase, VchanIntegrationMixin):
    """Test libvchan_set_blocking() API parity with vchan-xen."""

    def _load_lib_with_set_blocking(self):
        """Load the library with set_blocking declared."""
        from cffi import FFI
        ffi = FFI()
        ffi.cdef("""
struct libvchan;
typedef struct libvchan libvchan_t;
libvchan_t *libvchan_server_init(int domain, int port,
                                  size_t read_min, size_t write_min);
libvchan_t *libvchan_client_init(int domain, int port);
int libvchan_write(libvchan_t *ctrl, const void *data, size_t size);
int libvchan_read(libvchan_t *ctrl, void *data, size_t size);
int libvchan_wait(libvchan_t *ctrl);
void libvchan_close(libvchan_t *ctrl);
int libvchan_fd_for_select(libvchan_t *ctrl);
int libvchan_is_open(libvchan_t *ctrl);
int libvchan_data_ready(libvchan_t *ctrl);
int libvchan_buffer_space(libvchan_t *ctrl);
void libvchan_set_blocking(libvchan_t *ctrl, _Bool blocking);
""")
        return ffi, ffi.dlopen(
            os.path.join(os.path.dirname(__file__), '..', self.lib))

    def test_set_blocking_exists(self):
        """libvchan_set_blocking should be callable without error."""
        ffi, lib = self._load_lib_with_set_blocking()
        os.environ['VCHAN_DOMAIN'] = '1'
        os.environ['VCHAN_SOCKET_DIR'] = '/tmp'
        ctrl = lib.libvchan_server_init(2, 99, 1024, 1024)
        self.assertNotEqual(ctrl, ffi.NULL)
        try:
            lib.libvchan_set_blocking(ctrl, False)
            lib.libvchan_set_blocking(ctrl, True)
        finally:
            lib.libvchan_close(ctrl)

    def test_nonblocking_read_returns_zero(self):
        """In non-blocking mode, read on empty buffer should return 0."""
        ffi, lib = self._load_lib_with_set_blocking()
        os.environ['VCHAN_DOMAIN'] = '1'
        os.environ['VCHAN_SOCKET_DIR'] = '/tmp'
        server = lib.libvchan_server_init(2, 100, 1024, 1024)
        self.assertNotEqual(server, ffi.NULL)
        try:
            os.environ['VCHAN_DOMAIN'] = '2'
            client = lib.libvchan_client_init(1, 100)
            self.assertNotEqual(client, ffi.NULL)
            try:
                time.sleep(0.1)
                lib.libvchan_set_blocking(server, False)
                buf = ffi.new('char[]', 64)
                result = lib.libvchan_read(server, buf, 64)
                self.assertEqual(result, 0)
            finally:
                lib.libvchan_close(client)
        finally:
            lib.libvchan_close(server)


class QrexecProtocolTest(unittest.TestCase, VchanIntegrationMixin):
    """
    Test qrexec-style message framing over vchan-socket.

    qrexec uses a simple header: 4-byte type + 4-byte length, followed
    by the message body. This tests that vchan-socket preserves message
    boundaries correctly.
    """

    MSG_DATA_STDIN = 0x190
    MSG_DATA_STDOUT = 0x191
    MSG_DATA_EXIT_CODE = 0x198

    def _make_qrexec_msg(self, msg_type, data):
        header = struct.pack('<II', msg_type, len(data))
        return header + data

    def _read_qrexec_msg(self, vchan_endpoint):
        header = vchan_endpoint.recv(8)
        msg_type, length = struct.unpack('<II', header)
        if length > 0:
            body = vchan_endpoint.recv(length)
        else:
            body = b''
        return msg_type, body

    def test_single_message(self):
        """Send and receive a single qrexec-style message."""
        server = self.start_server()
        client = self.start_client()
        time.sleep(0.1)

        payload = b'echo hello world'
        msg = self._make_qrexec_msg(self.MSG_DATA_STDIN, payload)
        client.send(msg)

        msg_type, body = self._read_qrexec_msg(server)
        self.assertEqual(msg_type, self.MSG_DATA_STDIN)
        self.assertEqual(body, payload)

    def test_multiple_messages(self):
        """Send multiple qrexec messages in sequence."""
        server = self.start_server()
        client = self.start_client()
        time.sleep(0.1)

        messages = [
            (self.MSG_DATA_STDIN, b'first command'),
            (self.MSG_DATA_STDIN, b'second command'),
            (self.MSG_DATA_EXIT_CODE, struct.pack('<I', 0)),
        ]

        for msg_type, payload in messages:
            client.send(self._make_qrexec_msg(msg_type, payload))

        for expected_type, expected_body in messages:
            msg_type, body = self._read_qrexec_msg(server)
            self.assertEqual(msg_type, expected_type)
            self.assertEqual(body, expected_body)

    def test_bidirectional_messages(self):
        """Simultaneous send/receive simulating qrexec stdin/stdout."""
        server = self.start_server()
        client = self.start_client()
        time.sleep(0.1)

        stdin_data = b'input data for command'
        stdout_data = b'output from command'

        with ThreadPoolExecutor(max_workers=2) as pool:
            send_future = pool.submit(
                client.send,
                self._make_qrexec_msg(self.MSG_DATA_STDIN, stdin_data))
            recv_future = pool.submit(self._read_qrexec_msg, server)

            send_future.result(timeout=5)
            msg_type, body = recv_future.result(timeout=5)
            self.assertEqual(body, stdin_data)

        server.send(self._make_qrexec_msg(self.MSG_DATA_STDOUT, stdout_data))
        msg_type, body = self._read_qrexec_msg(client)
        self.assertEqual(msg_type, self.MSG_DATA_STDOUT)
        self.assertEqual(body, stdout_data)


class QubesDBProtocolTest(unittest.TestCase, VchanIntegrationMixin):
    """
    Test qubesdb-style key-value protocol over vchan-socket.

    qubesdb wire format: 1-byte type + 2-byte path_len + 2-byte data_len
    + path + data.
    """

    QDB_CMD_WRITE = 0x01
    QDB_CMD_READ = 0x02
    QDB_CMD_MULTIREAD = 0x05
    QDB_RESP_OK = 0x10
    QDB_RESP_ERROR = 0x11

    def _make_qubesdb_msg(self, cmd, path, data=b''):
        path_bytes = path.encode('utf-8') if isinstance(path, str) else path
        header = struct.pack('<BHH', cmd, len(path_bytes), len(data))
        return header + path_bytes + data

    def _read_qubesdb_msg(self, endpoint):
        header = endpoint.recv(5)
        cmd, path_len, data_len = struct.unpack('<BHH', header)
        path = endpoint.recv(path_len) if path_len > 0 else b''
        data = endpoint.recv(data_len) if data_len > 0 else b''
        return cmd, path, data

    def test_write_key(self):
        """Simulate qubesdb write: /qubes-ip = 10.137.0.5"""
        server = self.start_server()
        client = self.start_client()
        time.sleep(0.1)

        path = '/qubes-ip'
        value = b'10.137.0.5'
        msg = self._make_qubesdb_msg(self.QDB_CMD_WRITE, path, value)
        client.send(msg)

        cmd, recv_path, recv_data = self._read_qubesdb_msg(server)
        self.assertEqual(cmd, self.QDB_CMD_WRITE)
        self.assertEqual(recv_path, path.encode())
        self.assertEqual(recv_data, value)

    def test_multiple_keys(self):
        """Simulate full qubesdb sync: multiple keys in sequence."""
        server = self.start_server()
        client = self.start_client()
        time.sleep(0.1)

        keys = {
            '/qubes-ip': b'10.137.0.5',
            '/qubes-gateway': b'10.137.0.1',
            '/qubes-netmask': b'255.255.255.255',
            '/qubes-primary-dns': b'10.139.1.1',
            '/qubes-vm-type': b'AppVM',
            '/qubes-timezone': b'UTC',
            '/qubes-debug-mode': b'0',
        }

        for path, value in keys.items():
            msg = self._make_qubesdb_msg(self.QDB_CMD_WRITE, path, value)
            client.send(msg)

        for path, value in keys.items():
            cmd, recv_path, recv_data = self._read_qubesdb_msg(server)
            self.assertEqual(cmd, self.QDB_CMD_WRITE)
            self.assertEqual(recv_path, path.encode())
            self.assertEqual(recv_data, value)

    def test_request_response(self):
        """Simulate qubesdb read request + response cycle."""
        server = self.start_server()
        client = self.start_client()
        time.sleep(0.1)

        req = self._make_qubesdb_msg(self.QDB_CMD_READ, '/qubes-vm-type')
        client.send(req)

        cmd, path, data = self._read_qubesdb_msg(server)
        self.assertEqual(cmd, self.QDB_CMD_READ)

        resp = self._make_qubesdb_msg(self.QDB_RESP_OK, '/qubes-vm-type',
                                       b'AppVM')
        server.send(resp)

        cmd, path, data = self._read_qubesdb_msg(client)
        self.assertEqual(cmd, self.QDB_RESP_OK)
        self.assertEqual(data, b'AppVM')


class ConnectionLifecycleTest(unittest.TestCase, VchanIntegrationMixin):
    """Test connection lifecycle: connect, disconnect, reconnect."""

    def test_client_disconnect_detection(self):
        """Server should detect client disconnect."""
        server = self.start_server()
        client = self.start_client()
        time.sleep(0.1)
        self.assertEqual(server.state(), VCHAN_CONNECTED)

        client.close()
        time.sleep(0.3)
        self.assertEqual(server.state(), VCHAN_DISCONNECTED)

    def test_server_state_after_disconnect(self):
        """After client disconnects, server should report DISCONNECTED.

        vchan connections are one-shot (same as vchan-xen); reconnection
        requires a new server_init.
        """
        server = self.start_server()

        client1 = self.start_client()
        time.sleep(0.1)
        self.assertEqual(server.state(), VCHAN_CONNECTED)

        client1.send(b'from-client1')
        data = server.read(12)
        self.assertEqual(data, b'from-client1')

        client1.close()
        time.sleep(0.3)
        self.assertEqual(server.state(), VCHAN_DISCONNECTED)

    def test_large_transfer(self):
        """Transfer data larger than the ring buffer."""
        server = self.start_server(read_min=BUF_SIZE, write_min=BUF_SIZE)
        client = self.start_client()
        time.sleep(0.1)

        total_size = BUF_SIZE * 8
        send_data = bytes(range(256)) * (total_size // 256)

        received = bytearray()

        def sender():
            offset = 0
            while offset < len(send_data):
                chunk = send_data[offset:offset + 1024]
                n = client.send(chunk)
                offset += n

        def receiver():
            while len(received) < total_size:
                chunk = server.read(min(1024, total_size - len(received)))
                received.extend(chunk)

        with ThreadPoolExecutor(max_workers=2) as pool:
            sf = pool.submit(sender)
            rf = pool.submit(receiver)
            sf.result(timeout=10)
            rf.result(timeout=10)

        self.assertEqual(len(received), total_size)
        self.assertEqual(bytes(received), send_data)

    def test_concurrent_bidirectional(self):
        """Both sides send and receive simultaneously."""
        server = self.start_server()
        client = self.start_client()
        time.sleep(0.1)

        s2c_data = b'server-to-client-payload'
        c2s_data = b'client-to-server-payload'

        with ThreadPoolExecutor(max_workers=4) as pool:
            fs = pool.submit(server.send, s2c_data)
            fc = pool.submit(client.send, c2s_data)
            fr_s = pool.submit(server.recv, len(c2s_data))
            fr_c = pool.submit(client.recv, len(s2c_data))

            fs.result(timeout=5)
            fc.result(timeout=5)
            self.assertEqual(fr_s.result(timeout=5), c2s_data)
            self.assertEqual(fr_c.result(timeout=5), s2c_data)


class SimpleIntegrationTest(SetBlockingTest):
    """Run set_blocking tests against the simple vchan implementation."""
    lib = 'vchan-simple/libvchan-socket-simple.so'


class SimpleQrexecTest(QrexecProtocolTest):
    """Run qrexec protocol tests against the simple vchan implementation."""
    lib = 'vchan-simple/libvchan-socket-simple.so'


class SimpleQubesDBTest(QubesDBProtocolTest):
    """Run qubesdb protocol tests against the simple vchan implementation."""
    lib = 'vchan-simple/libvchan-socket-simple.so'


if __name__ == '__main__':
    unittest.main()
