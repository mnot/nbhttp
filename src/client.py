#!/usr/bin/env python

"""
asynchronous HTTP client library
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

from urlparse import urlsplit, urlunsplit

import push_tcp
from common import HttpMessageParser, \
    CLOSE, COUNTED, CHUNKED, NONE, \
    WAITING, HEADERS_DONE, \
    idempotent_methods, no_body_status, hop_by_hop_hdrs, \
    linesep, dummy
from error import *

# TODO: pipelining
# TODO: proxy support

class Client(HttpMessageParser):
    retry_limit = 2

    def __init__(self, res_start_cb):
        HttpMessageParser.__init__(self)
        self.res_start_cb = res_start_cb
        self.res_body_cb = None
        self.res_end_cb = None
        self.method = None
        self.uri = None
        self.req_hdrs = []
        self._tcp_conn = None
        self._conn_reusable = False
        self._req_state = WAITING
        self._req_content_length = None
        self._req_buffer = []
        self._req_body_pause_cb = None
        self._req_body_sent = 0
        self._retries = 0
        
    def req_start(self, method, uri, req_hdrs, req_body_pause):
        """
        Start a request to uri using method, where 
        req_hdrs is a list of (field_name, field_value) for
        the request headers.
        
        Returns a (req_body, req_done) tuple.
        """
        assert self._req_state == WAITING
        self._req_body_pause_cb = req_body_pause
        (scheme, authority, path, query, fragment) = urlsplit(uri)
        if scheme.lower() != 'http':
            self._handle_error(ERR_URL)
            return dummy, dummy
        if ":" in authority:
            host, port = authority.rsplit(":", 1)
            try:
                port = int(port)
            except ValueError:
                self._handle_error(ERR_URL)
                return dummy, dummy
        else:
            host, port = authority, 80
        if path == "":
            path = "/"
        # clean req headers
        req_hdrs = [i for i in req_hdrs \
                    if not i[0].lower() in ['host'] + hop_by_hop_hdrs ]
        try:
            self._req_content_length = int([i[1] for i in req_hdrs \
                                    if i[0].lower() == 'content-length'].pop(0))
        except IndexError:
            self._req_content_length = None
        req_hdrs.append(("Host", authority))
        req_hdrs.append(("Connection", "keep-alive"))
        uri = urlunsplit(('', '', path, query, ''))
        self.method, self.uri, self.req_hdrs = method, uri, req_hdrs
        self._req_state = HEADERS_DONE
        _idle_pool.attach(host, port, self._handle_connect, self._handle_connect_error)
        return self.req_body, self.req_done

    def req_body(self, data):
        """
        Write data to the request body.
        """
        assert self._req_state == HEADERS_DONE
        if not data: return
        if self._req_content_length == None: # TODO: chunked requests
            return self._handle_error(ERR_CL_REQ)
        self._req_buffer.append(data)
        if self._tcp_conn and self._tcp_conn.tcp_connected:
            self._tcp_conn.write("".join(self._req_buffer))
        self._req_body_sent += len(data)
        assert self._req_body_sent <= self._req_content_length, \
            "Too many request body bytes sent"
        
    def req_done(self, err=None):
        """
        Indicate that the request body is done.
        """
        assert self._req_state == HEADERS_DONE
        self._req_state = WAITING

    def res_body_pause(self, paused):
        """
        Temporarily stop sending the response body.
        """
        if self._tcp_conn and self._tcp_conn.tcp_connected:
            self._tcp_conn.pause(paused)
        
    # Methods called by push_tcp

    def _handle_connect(self, tcp_conn):
        self._tcp_conn = tcp_conn
        hdr_block = ["%s %s HTTP/1.1" % (self.method, self.uri)]
        hdr_block += ["%s: %s" % (k, v) for k, v in self.req_hdrs]
        hdr_block.append("")
        hdr_block.append("")
        self._tcp_conn.write(linesep.join(hdr_block))
        if self._req_buffer: # TODO: combine into single write
            self._tcp_conn.write("".join(self._req_buffer))
            self._req_buffer = []
        return self._handle_input, self._conn_closed, self._req_body_pause

    def _handle_connect_error(self, host, port, err):
        import os, types, socket
        if type(err) == types.IntType:
            err = os.strerror(err)
        elif isinstance(err, socket.error):
            err = err[1]
        else:
            err = str(err)
        self._handle_error(ERR_CONNECT, err)

    def _conn_closed(self):
        if self._input_buffer:
            self._handle_input("")
        if self._input_state == WAITING:
            return # we've seen the whole body already, or nothing has happened yet.
        elif self._input_delimit == CLOSE:
            self._input_state = WAITING
            self._input_end()
        else:
            if self.method in idempotent_methods and \
                self._retries < self.retry_limit and \
                self._input_state == WAITING:
                self._retries += 1
                _idle_pool.attach(self._tcp_conn.host, self._tcp_conn.port, 
                    self._handle_connect, self._handle_connect_error)                
            else:
                self._input_end((ERR_CONNECT, "Server closed the connection."))

    def _req_body_pause(self, paused):
        if self._req_body_pause_cb:
            self._req_body_pause_cb(paused)

    # Methods called by common.HttpRequestParser

    def _input_start(self, top_line, hdr_tuples, conn_tokens, transfer_codes, content_length):
        try: 
            res_version, status_txt = top_line.split(None, 1)
            res_version = float(res_version.rsplit('/', 1)[1])
            # TODO: check that the protocol is HTTP
        except (ValueError, IndexError):
            raise ValueError
        try:
            res_code, res_phrase = status_txt.split(None, 1)
        except ValueError:
            res_code = status_txt
            res_phrase = ""
        if 'close' not in conn_tokens:
            if (res_version == 1.0 and 'keep-alive' in conn_tokens) or \
                res_version > 1.0:
                self._conn_reusable = True
        self.res_body_cb, self.res_end_cb = self.res_start_cb(
                        res_version, res_code, res_phrase, hdr_tuples, self.res_body_pause)
        allows_body = (res_code not in no_body_status) or (self.method == "HEAD")
        return allows_body 

    def _input_body(self, chunk):
        self.res_body_cb(chunk)
        
    def _input_end(self, err=None, detail=None):
        if err and detail:
            err['detail'] = detail
        if self._tcp_conn:
            if self._tcp_conn.tcp_connected and self._conn_reusable and err is None:
                # Note that we don't reset read_cb; if more bytes come in before
                # the next request, we'll still get them.
                _idle_pool.release(self._tcp_conn)
            else:
                self._tcp_conn.close()
                self._tcp_conn = None
        self.res_end_cb(err)

    # misc

    def _handle_error(self, err, detail):
        assert self._input_state == WAITING
        self._input_delimit = CLOSE
        status_code, status_phrase = err.get('status', ('504', 'Gateway Timeout'))
        hdrs = [
            ('Content-Type', 'text/plain'),
            ('Connection', 'close'),
        ]
        body = err['desc']
        self.res_body_cb, self.res_end_cb = self.res_start_cb(
              "1.1", status_code, status_phrase, hdrs, dummy)
        self.res_body_cb(str(body))
        self._input_end(err, detail)


class _HttpConnectionPool:
    """
    A pool of idle TCP connections for use by the client.
    """
    connect_timeout = 3
    _conns = {}

    def attach(self, host, port, handle_connect, handle_connect_error):
        """
        Find an idle connection for (host, port), or create a new one.
        """
        while True:
            try:
                tcp_conn = self._conns[(host, port)].pop()
            except (IndexError, KeyError):
                push_tcp.create_client(host, port, handle_connect, handle_connect_error, 
                                       self.connect_timeout)
                break        
            if tcp_conn.tcp_connected:
                tcp_conn.read_cb, tcp_conn.close_cb, tcp_conn.pause_cb = \
                    handle_connect(tcp_conn)
                break
        
    def release(self, tcp_conn):
        """
        Add an idle connection back to the pool.
        """
        if tcp_conn.tcp_connected:
            def idle_close():
                try:
                    self._conns[(tcp_conn.host, tcp_conn.port)].remove(tcp_conn)
                except ValueError:
                    pass
            tcp_conn.close_cb = idle_close
            if not self._conns.has_key((tcp_conn.host, tcp_conn.port)):
                self._conns[(tcp_conn.host, tcp_conn.port)] = [tcp_conn]
            else:
                self._conns[(tcp_conn.host, tcp_conn.port)].append(tcp_conn)

_idle_pool = _HttpConnectionPool()


def test(request_uri):
    def printer(version, status, phrase, headers, res_pause):
        print "HTTP/%s" % version, status, phrase
        print "\n".join(["%s:%s" % header for header in headers])
        print
        def body(chunk):
            print chunk
        def done(err):
            if err:
                print "*** ERROR: %s (%s)" % (err['desc'], err['detail'])
            push_tcp.stop()
        return body, done
    def req_pause(paused):
        pass
    c = Client(printer)
    req_body_write, req_body_done = c.req_start("GET", request_uri, [], req_pause)
    req_body_done(True)
    push_tcp.run()
            
if __name__ == "__main__":
    import sys
    test(sys.argv[1])
