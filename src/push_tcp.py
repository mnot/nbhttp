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
Copyright (c) 2008-2010 Mark Nottingham

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

import asyncore
import bisect
import errno
import os
import sys
import socket
import time

try:
    import event      # http://www.monkey.org/~dugsong/pyevent/
except ImportError:
    event = None

class _TcpConnection(asyncore.dispatcher):
    "Base class for a TCP connection."
    write_bufsize = 16
    read_bufsize = 1024 * 16
    def __init__(self, sock, host, port):
        self.socket = sock
        self.host = host
        self.port = port
        self.read_cb = None
        self.close_cb = None
        self._close_cb_called = False
        self.pause_cb = None  
        self.tcp_connected = True # we assume a connected socket
        self._paused = False # TODO: should be paused by default
        self._closing = False
        self._write_buffer = []
        if event:
            self._revent = event.read(sock, self.handle_read)
            self._wevent = event.write(sock, self.handle_write)
        else: # asyncore
            asyncore.dispatcher.__init__(self, sock)

    def __repr__(self):
        status = [self.__class__.__module__+"."+self.__class__.__name__]
        if self.tcp_connected:
            status.append('connected')
        status.append('%s:%s' % (self.host, self.port))
        if event:
            status.append('event-based')
        if self._paused:
            status.append('paused')
        if self._closing:
            status.append('closing')
        if self._close_cb_called:
            status.append('close cb called')
        if self._write_buffer:
            status.append('%s write buffered' % len(self._write_buffer))
        return "<%s at %#x>" % (", ".join(status), id(self))

    def handle_connect(self): # asyncore
        pass
        
    def handle_read(self):
        """
        The connection has data read for reading; call read_cb
        if appropriate.
        """
        try:
            data = self.socket.recv(self.read_bufsize)
        except socket.error, why:
            if why[0] in [errno.EBADF, errno.ECONNRESET, errno.ESHUTDOWN, 
                          errno.ECONNABORTED, errno.ECONNREFUSED, 
                          errno.ENOTCONN, errno.EPIPE]:
                self.conn_closed()
                return
            else:
                raise
        if data == "":
            self.conn_closed()
        else:
            self.read_cb(data)
            if event:
                if self.read_cb and self.tcp_connected and not self._paused:
                    return self._revent
        
    def handle_write(self):
        "The connection is ready for writing; write any buffered data."
        if len(self._write_buffer) > 0:
            data = "".join(self._write_buffer)
            try:
                sent = self.socket.send(data)
            except socket.error, why:
                if why[0] == errno.EWOULDBLOCK:
                    return
                elif why[0] in [errno.EBADF, errno.ECONNRESET, 
                                errno.ESHUTDOWN, errno.ECONNABORTED,
                                errno.ECONNREFUSED, errno.ENOTCONN, 
                                errno.EPIPE]:
                    self.conn_closed()
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
        if event:
            if self.tcp_connected \
            and (len(self._write_buffer) > 0 or self._closing):
                return self._wevent

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
    handle_close = conn_closed # for asyncore

    def write(self, data):
        "Write data to the connection."
#        assert not self._paused
        self._write_buffer.append(data)
        if self.pause_cb and len(self._write_buffer) > self.write_bufsize:
            self.pause_cb(True)
        if event:
            if not self._wevent.pending():
                self._wevent.add()

    def pause(self, paused):
        """
        Temporarily stop/start reading from the connection and pushing
        it to the app.
        """
        if event:
            if paused:
                if self._revent.pending():
                    self._revent.delete()
            else:
                if not self._revent.pending():
                    self._revent.add()
        self._paused = paused

    def close(self):
        "Flush buffered data (if any) and close the connection."
        self.pause(True)
        if len(self._write_buffer) > 0:
            self._closing = True
        else:
            self.tcp_connected = False
            if event:
                if self._revent.pending():
                    self._revent.delete()
                if self._wevent.pending():
                    self._wevent.delete()
                self.socket.close()
            else:
                asyncore.dispatcher.close(self)

    def readable(self):
        "asyncore-specific readable method"
        return self.read_cb and self.tcp_connected and not self._paused
    
    def writable(self):
        "asyncore-specific writable method"
        return self.tcp_connected and \
            (len(self._write_buffer) > 0 or self._closing)

    def handle_error(self):
        """
        asyncore-specific misc error method. We treat it as if 
        the connection was closed.
        """
        self.conn_closed()


def create_server(host, port, conn_handler):
    """Listen to host:port and send connections to conn_handler."""
    sock = server_listen(host, port)
    attach_server(host, port, sock, conn_handler)

def server_listen(host, port):
    "Return a socket listening to host:port."
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setblocking(0)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(socket.SOMAXCONN)
    return sock
    
class attach_server(asyncore.dispatcher):
    "Attach a server to a listening socket."
    def __init__(self, host, port, sock, conn_handler):
        self.host = host
        self.port = port
        self.conn_handler = conn_handler
        if event:
            event.event(self.handle_accept, handle=sock,
                        evtype=event.EV_READ|event.EV_PERSIST).add()
        else: # asyncore
            asyncore.dispatcher.__init__(self, sock=sock)
            self.accepting = True

    def handle_accept(self, *args):
        try:
            if event:
                conn, addr = args[1].accept()
            else: # asyncore
                conn, addr = self.accept()
        except TypeError: 
            # sometimes accept() returns None if we have 
            # multiple processes listening
            return
        tcp_conn = _TcpConnection(conn, self.host, self.port)
        tcp_conn.read_cb, tcp_conn.close_cb, tcp_conn.pause_cb = \
            self.conn_handler(tcp_conn)

    def handle_error(self):
        stop() # FIXME: handle unscheduled errors more gracefully
        raise

class create_client(asyncore.dispatcher):
    "An asynchronous TCP client."
    def __init__(self, host, port, conn_handler, 
        connect_error_handler, connect_timeout=None):
        self.host = host
        self.port = port
        self.conn_handler = conn_handler
        self.connect_error_handler = connect_error_handler
        self._timeout_ev = None
        self._error_sent = False
        # TODO: socket.getaddrinfo(); needs to be non-blocking.
        if event:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(0)
            event.write(sock, self.handle_connect, sock).add()
            try:
                # FIXME: check for DNS errors, etc.
                err = sock.connect_ex((host, port))
            except socket.error, why:
                self.handle_conn_error()
                return
            except socket.gaierror, why:
                self.handle_conn_error()
                return
            if err != errno.EINPROGRESS: # FIXME: others?
                self.handle_conn_error((err, os.strerror(err)))
                return
        else: # asyncore
            asyncore.dispatcher.__init__(self)
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self.connect((host, port)) 
                # exceptions should be caught by handle_error
            except socket.error, why:
                self.handle_conn_error()
                return
            except socket.gaierror, why:
                self.handle_conn_error()
                return
        if connect_timeout:
            self._timeout_ev = schedule(connect_timeout,
                            self.connect_error_handler, 
                            (errno.ETIMEDOUT, os.strerror(errno.ETIMEDOUT))
            )

    def handle_connect(self, sock=None):
        if self._timeout_ev:
            self._timeout_ev.delete()
        if self._error_sent:
            return
        if sock is None: # asyncore
            sock = self.socket
        tcp_conn = _TcpConnection(sock, self.host, self.port)
        tcp_conn.read_cb, tcp_conn.close_cb, tcp_conn.pause_cb = \
            self.conn_handler(tcp_conn)

    def handle_read(self): # asyncore
        pass

    def handle_write(self): # asyncore
        pass

    def handle_conn_error(self, ex_value=None):
        if ex_value is None:
            ex_type, ex_value = sys.exc_info()[:2]
        else:
            ex_type = socket.error
        if ex_type in [socket.error, socket.gaierror]:
            if ex_value[0] == errno.ECONNREFUSED:
                return # OS will retry
            if self._timeout_ev:
                self._timeout_ev.delete()
            if self._error_sent:
                return
            elif self.connect_error_handler:
                self._error_sent = True
                self.connect_error_handler(ex_value)
        else:
            if self._timeout_ev:
                self._timeout_ev.delete()
            raise
    
    def handle_error(self):
        stop() # FIXME: handle unscheduled errors more gracefully
        raise


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
        self._now = None
        self._running = False

    def run(self):
        "Start the loop."
        last_event_check = 0
        self._running = True
        while (self.socket_map or self.events) and self._running:
            self._now = time.time()
            if (self._now - last_event_check) >= self.granularity:
                last_event_check = self._now
                for event in self.events:
                    when, what = event
                    if self._now >= when:
                        try:
                            self.events.remove(event)
                        except ValueError: 
                            # a previous event may have removed this one.
                            continue
                        what()
                    else:
                        break
            # sample the number of channels
            n = len(self.socket_map)
            self.num_channels = n
            if n > self.max_channels:
                self.max_channels = n
            asyncore.poll(self.timeout) # TODO: use poll2 when available
            
    def stop(self):
        "Stop the loop."
        self.socket_map.clear()
        self.events = []
        self._now = None
        self._running = False
            
    def time(self):
        "Return the current time (to avoid a system call)."
        return self._now or time.time()

    def schedule(self, delta, callback, *args):
        "Schedule callable callback to be run in delta seconds with *args."
        def cb():
            if callback:
                callback(*args)
        new_event = (self.time() + delta, cb)
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

_event_running = False
def _event_run(*args):
    _event_running = True
    event.dispatch(*args)

def _event_stop(*args):
    _event_running = False
    event.abort(*args)

if event:
    schedule = event.timeout
    run = _event_run
    stop =  _event_stop
    now = time.time
    running = _event_running
else:
    _loop = _AsyncoreLoop()
    schedule = _loop.schedule
    run = _loop.run
    stop = _loop.stop
    now = _loop.time
    running = _loop.running
