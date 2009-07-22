#!/usr/bin/env python

"""
push-based asynchronous TCP

This is a generic library for building event-based / asynchronous
TCP servers and clients. 

By default, it uses the asyncore library included with Python. 
However, if the pyevent library 
<http://www.monkey.org/~dugsong/pyevent/> is available, it will 
use that, offering higher concurrency and, perhaps, performance.

It uses a push model; i.e., the network connection pushes data to
you (using a callback), and you push data to the network connection
(using a direct method invocation). 

*** Building Clients

To connect to a server, use create_client;
> host = 'www.example.com'
> port = '80'
> push_tcp.create_client(host, port, conn_handler, error_handler)

conn_handler will be called with the tcp_conn as the argument 
when the connection is made. See "Working with Connections" 
below for details.

error_handler will be called if the connection can't be made for some reason.

> def error_handler(host, port, reason):
>   print "can't connect to %s:%s: %s" % (host, port, reason)

*** Building Servers

To start listening, use create_server;

> server = push_tcp.create_server(host, port, conn_handler)

conn_handler is called every time a new client connects; see
"Working with Connections" below for details.

The server object itself keeps track of all of the open connections, and
can be used to do things like idle connection management, etc.

*** Working with Connections

Every time a new connection is established -- whether as a client
or as a server -- the conn_handler given is called with tcp_conn
as its argument;

> def conn_handler(tcp_conn):
>   print "connected to %s:%s" % tcp_conn.host, tcp_conn.port
>   return read_cb, close_cb, pause_cb

It must return a (read_cb, close_cb, pause_cb) tuple.

read_cb will be called every time incoming data is available from
the connection;

> def read_cb(data):
>   print "got some data:", data

When you want to write to the connection, just write to it:

> tcp_conn.write(data)

If you want to close the connection from your side, just call close:

> tcp_conn.close()

Note that this will flush any data already written.

If the other side closes the connection, close_cb will be called;

> def close_cb():
>   print "oops, they don't like us any more..."

If you write too much data to the connection and the buffers fill up, 
pause_cb will be called with True to tell you to stop sending data 
temporarily;

> def pause_cb(paused):
>   if paused:
>       # stop sending data
>   else:
>       # it's OK to start again

Note that this is advisory; if you ignore it, the data will still be
buffered, but the buffer will grow.

Likewise, if you want to pause the connection because your buffers 
are full, call pause;

> tcp_conn.pause(True)

but don't forget to tell it when it's OK to send data again;

> tcp_conn.pause(False)

*** Timed Events

It's often useful to schedule an event to be run some time in the future;

> push_tcp.schedule(10, cb, "foo")

This example will schedule the function 'cb' to be called with the argument
"foo" ten seconds in the future.

*** Running the loop

In all cases (clients, servers, and timed events), you'll need to start
the event loop before anything actually happens;

> push_tcp.run()

To stop it, just stop it;

> push_tcp.stop()
"""

__author__ = "Mark Nottingham <mnot@mnot.net>"
__copyright__ = """\
Copyright (c) 2008-2009 Mark Nottingham

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import sys
import socket
import errno
import asyncore
import time
import bisect

try:
    import event      # http://www.monkey.org/~dugsong/pyevent/
except ImportError:
    event = None

class _TcpConnectionBase(object):
    "Base class for a TCP connection."
    write_bufsize = 16
    read_bufsize = 1024 * 16
    def __init__(self, sock, host, port, connect_error_handler=None):
        self.sockit = sock
        self.host = host
        self.port = port
        self.connect_error_handler = connect_error_handler
        self.read_cb = None
        self.close_cb = None
        self._close_cb_called = False
        self.pause_cb = None  
        self.tcp_connected = True # always handed a connected socket (we assume)
        self._paused = False # TODO: should be paused by default
        self._closing = False
        self._write_buffer = []

    def conn_readable(self):
        """
        The connection has data read for reading; call read_cb
        if appropriate.
        """
        try:
            data = self.sockit.recv(self.read_bufsize)
        except socket.error, why:
            if why[0] in [errno.EBADF, errno.ECONNRESET, errno.EPIPE, errno.ETIMEDOUT]:
                self.conn_closed()
                return
            elif why[0] in [errno.ECONNREFUSED, errno.ENETUNREACH] and self.connect_error_handler:
                self.tcp_connected = False
                self.connect_error_handler(why[0])
                return
            else:
                raise
        if data == "":
            self.conn_closed()
        else:
            self.read_cb(data)
        
    def conn_writable(self):
        "The connection is ready for writing; write any buffered data."
        if len(self._write_buffer) > 0:
            data = "".join(self._write_buffer)
            try:
                sent = self.sockit.send(data)
            except socket.error, why:
                if why[0] in [errno.EBADF, errno.ECONNRESET, errno.EPIPE, errno.ETIMEDOUT]:
                    self.conn_closed()
                    return
                elif why[0] in [errno.ECONNREFUSED, errno.ENETUNREACH] and \
                  self.connect_error_handler:
                    self.tcp_connected = False
                    self.connect_error_handler(why[0])
                    return
                else:
                    raise
            if sent < len(data):
                self._write_buffer = [data[sent:]]
            else:
                self._write_buffer = []
        if self.pause_cb and len(self._write_buffer) < self.write_bufsize:
            self.pause_cb(False)
        if self._closing:
            self.close()

    def conn_closed(self):
        """
        The connection has been closed by the other side. Do local cleanup
        and then call close_cb.
        """
        self.tcp_connected = False
        if self._close_cb_called:
            return
        elif self.close_cb:
            self._close_cb_called = True
            self.close_cb()
        else:
            # uncomfortable race condition here, so we try again.
            # not great, but ok for now. 
            schedule(1, self.conn_closed)

    def write(self, data):
        "Write data to the connection."
#        assert not self._paused
        self._write_buffer.append(data)
        if self.pause_cb and len(self._write_buffer) > self.write_bufsize:
            self.pause_cb(True)

    def pause(self, paused):
        """
        Temporarily stop/start reading from the connection and pushing
        it to the app.
        """
        self._paused = paused

    def close(self):
        "Flush buffered data (if any) and close the connection."
        self.pause(True)
        if len(self._write_buffer) > 0:
            self._closing = True
        else:
            self.sockit.close()
            self.tcp_connected = False


class _AsyncoreConnection(_TcpConnectionBase, asyncore.dispatcher):
    "A TCP connection using the asyncore library."
    def __init__(self, sock, host, port, error_handler):
        _TcpConnectionBase.__init__(self, sock, host, port, error_handler)
        asyncore.dispatcher.__init__(self, sock)

    def readable(self):
        return self.read_cb and self.tcp_connected and not self._paused

    def writable(self):
        return self.tcp_connected and (len(self._write_buffer) > 0 or self._closing)

    handle_read = _TcpConnectionBase.conn_readable
    handle_write = _TcpConnectionBase.conn_writable
    handle_close = _TcpConnectionBase.conn_closed
        
    def handle_error(self):
        err = sys.exc_info()
        if issubclass(err[0], socket.error):
            self.connect_error_handler(err[0])
        else:
            raise


class _EventConnection(_TcpConnectionBase):
    "A TCP connection using the event library."
    def __init__(self, sock, host, port, error_handler):
        _TcpConnectionBase.__init__(self, sock, host, port, error_handler)
        self._revent = event.read(sock, self.conn_readable)
        self._wevent = event.write(sock, self.conn_writable)

    def conn_readable(self):
        _TcpConnectionBase.conn_readable(self)
        if self.read_cb and self.tcp_connected and not self._paused:
            return self._revent

    def conn_writable(self):
        _TcpConnectionBase.conn_writable(self)
        if self.tcp_connected and (len(self._write_buffer) > 0 or self._closing):
            return self._wevent

    def write(self, data):
        _TcpConnectionBase.write(self, data)
        if not self._wevent.pending():
            self._wevent.add()

    def pause(self, paused):
        if paused:
            if self._revent.pending():
                self._revent.delete()
        else:
            if not self._revent.pending():
                self._revent.add()
        self._paused = paused
        

class _AsyncoreServer(asyncore.dispatcher):
    "A TCP server using the asyncore library."
    def __init__(self, host, port, conn_handler):
        asyncore.dispatcher.__init__(self)
        self.host = host
        self.port = port
        self.conn_handler = conn_handler
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        try:
            self.bind((host, port))
        except socket.error:
            raise
        self.listen(socket.SOMAXCONN) # TODO: set SO_SNDBUF, SO_RCVBUF
    
    def handle_accept(self):
        conn, addr = self.accept()
        tcp_conn = _AsyncoreConnection(conn, self.host, self.port, self.handle_error)
        tcp_conn.read_cb, tcp_conn.close_cb, tcp_conn.pause_cb = self.conn_handler(tcp_conn)

    def handle_error(self):
        raise AssertionError, "this kind of error should never happen for a server."


class _EventServer:
    "A TCP server using the event library."
    def __init__(self, host, port, conn_handler):
        self.host = host
        self.port = port
        self.conn_handler = conn_handler
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(0)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))  
        sock.listen(socket.SOMAXCONN)
        event.event(self.handle_connection, handle=sock,
                    evtype=event.EV_READ|event.EV_PERSIST).add()

    def handle_connection(self, ev, sock, evtype, arg):
        conn, addr = sock.accept() # FIXME: catch ECONNABORTED, EINVAL, EMFILE, ENFILE, ENOMEM, EWOULDBLOCK
        tcp_conn = _EventConnection(conn, self.host, self.port, self.handle_error)
        tcp_conn.read_cb, tcp_conn.close_cb, tcp_conn.pause_cb = self.conn_handler(tcp_conn)
        
    def handle_error(self):
        raise AssertionError, "this kind of error should never happen for a server."

        
class _AsyncoreClient(asyncore.dispatcher):
    "A TCP client using the asyncore library."
    def __init__(self, host, port, conn_handler, connect_error_handler, timeout=None):
        asyncore.dispatcher.__init__(self)
        self.host = host
        self.port = port
        self.conn_handler = conn_handler
        self.connect_error_handler = connect_error_handler
        self._timeout_ev = None
        self._conn_handled = False
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        if timeout:
            to_err = errno.ETIMEDOUT
            self._timeout_ev = schedule(timeout, self.handle_error, to_err)
        # TODO: socket.getaddrinfo(); needs to be non-blocking.
        try:
            self.connect((host, port))
        except socket.error, why:
            self.handle_error(why[0])

    def handle_connect(self):
        if not self._conn_handled:
            self._conn_handled = True
            if self._timeout_ev:
                self._timeout_ev.delete()
            tcp_conn = _AsyncoreConnection(self.socket, self.host, self.port, 
                                           self.handle_error)
            tcp_conn.read_cb, tcp_conn.close_cb, tcp_conn.pause_cb = self.conn_handler(tcp_conn)

    def handle_error(self, err=None):
        if not self._conn_handled:
            self._conn_handled = True
            if self._timeout_ev:
                self._timeout_ev.delete()
            if err == None:
                t, err, tb = sys.exc_info()
            self.connect_error_handler(self.host, self.port, err)

    def handle_read(self):
        pass

    def handle_write(self):
        pass

class _EventClient:
    "A TCP client using the event library."
    def __init__(self, host, port, conn_handler, connect_error_handler, timeout=None):
        self.host = host
        self.port = port
        self.conn_handler = conn_handler
        self.connect_error_handler = connect_error_handler
        self._timeout_ev = None
        self._error_sent = False
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(0)
        event.write(sock, self.handle_connect, sock).add()
        # TODO: socket.getaddrinfo(); needs to be non-blocking.
        if timeout:
            to_err = errno.ETIMEDOUT
            self._timeout_ev = schedule(timeout, self.handle_error, to_err)
        try:
            err = sock.connect_ex((host, port)) # FIXME: check for DNS errors, etc.
        except socket.error, why:
            if self._timeout_ev:
                self._timeout_ev.delete()
            self.handle_error(why)
            return
        if err != errno.EINPROGRESS: # FIXME: others?
            if self._timeout_ev:
                self._timeout_ev.delete()
            self.handle_error(err)
  
    def handle_connect(self, sock):
        if self._timeout_ev:
            self._timeout_ev.delete()
        tcp_conn = _EventConnection(sock, self.host, self.port, self.handle_error)
        tcp_conn.read_cb, tcp_conn.close_cb, tcp_conn.pause_cb = self.conn_handler(tcp_conn)

    def handle_error(self, err=None):
        if self._timeout_ev:
            self._timeout_ev.delete()
        if not self._error_sent:
            self._error_sent = True
            if err == None:
                t, err, tb = sys.exc_info()
            self.connect_error_handler(self.host, self.port, err)


# adapted from Medusa
class _AsyncoreLoop:
    "Asyncore main loop + event scheduling."
    def __init__(self):
        self.events = []
        self.num_channels = 0
        self.max_channels = 0
        self.timeout = 1
        self.granularity = 1
        self.socket_map = asyncore.socket_map

    def run(self):
        "Start the loop."
        last_event_check = 0
        while self.socket_map or self.events:
            now = time.time()
            if (now - last_event_check) >= self.granularity:
                last_event_check = now
                for event in self.events:
                    when, what = event
                    if now >= when:
                        self.events.remove(event)
                        what()
                    else:
                        break
            # sample the number of channels
            n = len(self.socket_map)
            self.num_channels = n
            if n > self.max_channels:
                self.max_channels = n
            asyncore.poll(self.timeout)
            
    def stop(self):
        "Stop the loop."
        self.socket_map = {}
        self.events = []
            
    def schedule(self, delta, callback, *args):
        "Schedule callable callback to be run in delta seconds with *args."
        def cb():
            if callback:
                callback(*args)
        new_event = (time.time() + delta, cb)
        events = self.events
        bisect.insort(events, new_event)
        class event_holder:
            def __init__(self):
                self._deleted = False
            def delete(self):
                if not self._deleted:
                    try:
                        events.remove(new_event)
                        self._deleted = True
                    except ValueError: # already gone
                        pass
        return event_holder()

if event:
    create_client = _EventClient
    create_server = _EventServer
    schedule = event.timeout
    run = event.dispatch
    stop =  event.abort
else:
    create_client = _AsyncoreClient
    create_server = _AsyncoreServer
    _loop = _AsyncoreLoop()
    schedule = _loop.schedule
    run = _loop.run
    stop = _loop.stop
