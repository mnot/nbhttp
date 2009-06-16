#!/usr/bin/env python

"""
nbhttp.server - asynchronous HTTP server library

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

import os
import sys
import logging

import push_tcp
from common import HttpMessageParser, \
    CLOSE, COUNTED, CHUNKED, NONE, \
    WAITING, HEADERS_DONE, \
    no_body_status, hop_by_hop_hdrs, \
    linesep, dummy

logging.basicConfig()
log = logging.getLogger('server')
log.setLevel(logging.WARNING)


class Server:
    def __init__(self, host, port, request_handler):
        self.request_handler = request_handler
        self.server = push_tcp.create_server(host, port, self.handle_connection)
        
    def handle_connection(self, tcp_conn):
        conn = HttpServerConnection(self.request_handler, tcp_conn)
        return conn._handle_input, conn._conn_closed, conn._res_body_pause


class HttpServerConnection(HttpMessageParser):
    def __init__(self, request_handler, tcp_conn):
        HttpMessageParser.__init__(self)
        self.request_handler = request_handler
        self.req_body_cb = None
        self.req_end_cb = None
        self.method = None
        self.req_version = None
        self._tcp_conn = tcp_conn
        self._res_state = WAITING
        self.connection_hdr = []
        self._res_delimit = None
        self._res_body_pause_cb = None

    def res_start(self, status_code, status_phrase, res_hdrs, res_body_pause):
        log.info("%s server res_start %s %s" % (id(self), status_code, status_phrase))
        assert self._res_state == WAITING, self._res_state
        self._res_body_pause_cb = res_body_pause
        res_top = ["HTTP/1.1 %s %s" % (status_code, status_phrase)]
        res_len = None
        for name, value in res_hdrs:
            norm_name = name.strip().lower()
            if norm_name == "content-length":
                try:
                    res_len = int(value)
                except ValueError:
                    raise
            if norm_name in hop_by_hop_hdrs:
                continue
            res_top.append("%s: %s" % (name, value))
        if "close" in self.connection_hdr:
            self._res_delimit = CLOSE
            res_top.append("Connection: close, asked_for")
        elif res_len is not None:
            self._res_delimit = COUNTED
            res_top.append("Content-Length: %s" % str(res_len))
            res_top.append("Connection: keep-alive")
        elif 2.0 > self.req_version >= 1.1:
            self._res_delimit = CHUNKED
            res_top.append("Transfer-Encoding: chunked")
        else:
            self._res_delimit = CLOSE
            res_top.append("Connection: close, default, %s" % self.req_version)
        res_top.append(linesep)
        self._tcp_conn.write(linesep.join(res_top))
        self._res_state = HEADERS_DONE
        return self.res_body, self.res_end

    def res_body(self, data):
        log.debug("%s server res_body %s " % (id(self), len(data)))
        assert self._res_state == HEADERS_DONE, self._res_state
        # TODO: if we sent C-L and we've written more than that many bytes, blow up.
        if self._res_delimit == CHUNKED:
            self._tcp_conn.write("%s\r\n%s\r\n" % (hex(len(data))[2:], data))
        else:
            self._tcp_conn.write("%s" % data)

    def res_end(self, complete=True):
        log.debug("%s server res_end" % id(self))
        assert self._res_state == HEADERS_DONE, self._res_state
        self._res_state = WAITING
        # TODO: if we sent C-L and we haven't written that many bytes, blow up.
        if self._res_delimit == NONE:
            pass
        if self._res_delimit == CHUNKED:
            self._tcp_conn.write("0\r\n\r\n") # We don't support trailers
        if self._res_delimit == CLOSE:
            self._tcp_conn.close()
        elif not complete:
            self._tcp_conn.close()
        # TODO: cleanup if prematurely aborted

    def req_body_pause(self, paused):
        if self._tcp_conn and self._tcp_conn.tcp_connected:
            self._tcp_conn.pause(paused)

    # Methods called by push_tcp

    def _res_body_pause(self, paused):
        if self._res_body_pause_cb:
            self._res_body_pause_cb(paused)

    def _conn_closed(self):
        if self._res_state != WAITING:
            pass # FIXME: any cleanup necessary?
#        self.pause()
#        self._queue = []
#        self.tcp_conn.handler = None
#        self.tcp_conn = None                    

    # Methods called by common.HttpRequestParser

    def _input_start(self, top_line, hdr_tuples, conn_tokens, transfer_codes, content_length):
        """
        Take the top set of headers from the input stream, parse them
        and queue the request to be processed by the application.
        """
        assert self._input_state == WAITING, "pipelining not supported" # FIXME: pipelining
        try: 
            method, _req_line = top_line.split(None, 1)
            uri, req_version = _req_line.rsplit(None, 1)
            self.req_version = float(req_version.rsplit('/', 1)[1])
        except ValueError, why:
            self._handle_error("400", "Bad Request", why)
            raise ValueError
        if req_version == 1.1 and 'host' not in [ k.strip().lower() for (k,v) in hdr_tuples]:
            self._handle_error("400", "Bad Request", "Host header required.")
            raise ValueError

        self.method = method
        self.connection_hdr = conn_tokens

        log.info("%s server req_start %s %s %s" % (id(self), method, uri, self.req_version))
        self.req_body_cb, self.req_end_cb = self.request_handler(
                method, uri, hdr_tuples, self.res_start, self.req_body_pause)
        allows_body = (content_length) or (transfer_codes != [])
        return req_version, allows_body

    def _input_body(self, chunk):
        self.req_body_cb(chunk)
    
    def _input_end(self, complete):
        self.req_end_cb(complete)
        
    def _input_extra(self, chunk):
        self._input_end(True) # Finish the outstanding request
        # FIXME: This is a request with a body that has extra; by definition
        # it can't be a pipelined request. Do we do anything else with the data?
         
    def _handle_error(self, status_code, status_phrase, body=""):
#        self._queue.append(ErrorHandler(status_code, status_phrase, body, self))
        self.res_start(status_code, status_phrase, [], dummy)
        self.res_body(body)
        self.res_end(False)
    
    
def test_handler(method, uri, hdrs, res_start, req_pause):
    code = "200"
    phrase = "OK"
    res_hdrs = [('Content-Type', 'text/plain')]
    def res_pause(paused):
        pass
    res_body, res_end = res_start(code, phrase, hdrs, res_pause)
    res_body('foo!')
    res_end(True)
    def req_body(data):
        pass
    def req_end(complete):
        pass
    return req_body, req_end
    
if __name__ == "__main__":
    sys.stderr.write("PID: %s\n" % os.getpid())
    h, p = sys.argv[1], int(sys.argv[2])
    server = Server(h, p, test_handler)
    push_tcp.run()
