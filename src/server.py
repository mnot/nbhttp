#!/usr/bin/env python

"""
Non-Blocking HTTP Server

This library allow implementation of an HTTP/1.1 server that is "non-blocking,"
"asynchronous" and "event-driven" -- i.e., it achieves very high performance
and concurrency, so long as the application code does not block (e.g.,
upon network, disk or database access). Blocking on one request will block
the entire server.

Instantiate a Server with the following parameters:
  - host (string)
  - port (int)
  - req_start (callable)
  
req_start is called when a request starts. It must take the following arguments:
  - method (string)
  - uri (string)
  - req_hdrs (list of (name, value) tuples)
  - res_start (callable)
  - req_body_pause (callable)
and return:
  - req_body (callable)
  - req_done (callable)
    
req_body is called when part of the request body is available. It must take the 
following argument:
  - chunk (string)

req_done is called when the request is complete, whether or not it contains a 
body. It must take the following argument:
  - err (error dictionary, or None for no error)

Call req_body_pause when you want the server to temporarily stop sending the 
request body, or restart. You must provide the following argument:
  - paused (boolean; True means pause, False means unpause)
    
Call res_start when you want to start the response, and provide the following 
arguments:
  - status_code (string)
  - status_phrase (string)
  - res_hdrs (list of (name, value) tuples)
  - res_body_pause
It returns:
  - res_body (callable)
  - res_done (callable)
    
Call res_body to send part of the response body to the client. Provide the 
following parameter:
  - chunk (string)
  
Call res_done when the response is finished, and provide the 
following argument if appropriate:
  - err (error dictionary, or None for no error)
    
See the error module for the complete list of valid error dictionaries.

Where possible, errors in the request will be responded to with the appropriate
4xx HTTP status code. However, if a response has already been started, the
connection will be dropped (for example, when the request chunking or
indicated length are incorrect).
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

import os
import sys
import logging

import push_tcp
from common import HttpMessageParser, \
    CLOSE, COUNTED, CHUNKED, NONE, \
    WAITING, HEADERS_DONE, \
    hop_by_hop_hdrs, \
    linesep, dummy

from error import ERR_HTTP_VERSION, ERR_HOST_REQ, ERR_TRANSFER_CODE, \
    ERR_WHITESPACE_HDR
from state_enforce import State

logging.basicConfig()
log = logging.getLogger('server')
log.setLevel(logging.DEBUG)

# FIXME: assure that the connection isn't closed before reading the entire req body
# TODO: filter out 100 responses to HTTP/1.0 clients that didn't ask for it.

class Server:
    "An asynchronous HTTP server."
    def __init__(self, host, port, request_handler):
        self.request_handler = request_handler
        self.server = push_tcp.create_server(host, port, self.handle_connection)
        
    def handle_connection(self, tcp_conn):
        "Process a new push_tcp connection, tcp_conn."
        conn = HttpServerConnection(self.request_handler, tcp_conn)
        return conn._handle_input, conn._conn_closed, conn._res_body_pause


class HttpServerConnection(HttpMessageParser):
    "A handler for an HTTP server connection."
    def __init__(self, request_handler, tcp_conn):
        HttpMessageParser.__init__(self)
        self.request_handler = request_handler
        self._tcp_conn = tcp_conn
        self._res_body_pause_cb = dummy
        self._queue = []

    def req_body_pause(self, paused):
        "Indicate that the server should pause (True) or unpause (False) the request."
        if self._tcp_conn and self._tcp_conn.tcp_connected:
            print "PAUSE" # FIXME: needs to be connected to the request
            self._tcp_conn.pause(paused)

    # Methods called by push_tcp

    def _res_body_pause(self, paused):
        "Pause/unpause sending the response body."
        self._res_body_pause_cb(paused)

    def _conn_closed(self):
        "The server connection has closed."
        # FIXME: any other cleanup necessary?
        pass

    # Methods called by common.HttpRequestParser

    @State('srv_conn', None, '_input_end')
    def _input_start(self, top_line, hdr_tuples, conn_tokens, transfer_codes, content_length):
        """
        Take the top set of headers from the input stream, parse them
        and queue the request to be processed by the application.
        """
        req = HttpServerRequest(self)
        req.connection_hdr = conn_tokens
        req.hdr_tuples = hdr_tuples
        self._queue.append(req)
        try: 
            req.method, _req_line = top_line.split(None, 1)
            req.uri, req_version = _req_line.rsplit(None, 1)
            req.req_version = float(req_version.rsplit('/', 1)[1])
        except (ValueError, IndexError), why:
            req._handle_error(ERR_HTTP_VERSION, top_line) # FIXME: more fine-grained
            raise ValueError
        if req.req_version == 1.1 and 'host' not in [ k.strip().lower() for (k, v) in hdr_tuples]:
            req._handle_error(ERR_HOST_REQ)
            raise ValueError
        if hdr_tuples[:1][:1][:1] in [" ", "\t"]:
            req._handle_error(ERR_WHITESPACE_HDR)
            raise ValueError
        for code in transfer_codes: # we only support 'identity' and chunked' codes
            if code not in ['identity', 'chunked']: 
                # FIXME: SHOULD also close connection
                req._handle_error(ERR_TRANSFER_CODE)
                raise ValueError
        # FIXME: MUST 400 request messages with whitespace between name and colon
        if len(self._queue) == 1:
            self._queue[0].req_start(self._tcp_conn)
        else:
            print "** pipeline queue"
        allows_body = (content_length) or (transfer_codes != [])
        print "queue add %s" % req.uri
        return allows_body

    @State('srv_conn', '_input_start', '_input_body')
    def _input_body(self, chunk):
        "Process a request body chunk from the wire."
        self._queue[-1].req_body(chunk)
    
    @State('srv_conn', '_input_start', '_input_body')
    def _input_end(self):
        "Indicate that the request is finished."
        self._queue[-1].req_done(None)

    @State('srv_conn', '_input_start', '_input_body')
    def _input_error(self, err, detail=None):
        "Indicate a parsing problem with the request."
        assert "input error"
        err['detail'] = detail
        if self._tcp_conn: # FIXME: flush queue up to error? send to client?
            self._tcp_conn.close()
            self._tcp_conn = None
        self._queue[-1].req_done(err)

    def res_done(self, req):
        print "conn res done..."
        finished_req = self._queue.pop(0)
        assert finished_req is req, "mismatched request %s to %s" % (finished_req.__dict__, req.__dict__)
        if len(self._queue) > 0:
            print "starting next req..."
            self._queue[0].req_start(self._tcp_conn)
        


class HttpServerRequest:
    def __init__(self, conn):
        self.request_handler = conn.request_handler
        self.req_body_pause = conn.req_body_pause
        self.conn_res_done = conn.res_done
        self.req_body_cb = None
        self.req_done_cb = None
        self.method = None
        self.uri = None
        self.req_version = None
        self.hdr_tuples = []
        self.connection_hdr = []
        self._tcp_conn = None
        self._res_state = WAITING
        self._res_delimit = None
        self._req_body_buffer = []
        self._req_done = False
        self._error = None

    @State('srv_res')
    def req_start(self, tcp_conn):
        "Kick off the request."
        log.info("%s server req_start %s %s %s" % (id(self), self.method, self.uri, self.req_version))
        self._tcp_conn = tcp_conn
        if not self._error:
            self.req_body_cb, self.req_done_cb = self.request_handler(
                self.method, self.uri, self.hdr_tuples, self.res_start, self.req_body_pause)
            for chunk in self._req_body_buffer:
                self.req_body_cb(chunk)
            if self._req_done:
                self.req_done_cb(self._req_done)
        else:
            status_code, status_phrase = self._error.get('status', ('400', 'Bad Request'))
            hdrs = [
                ('Content-Type', 'text/plain'),
            ]
            body = self._error['desc']
            if self._error.has_key('detail'):
                body += " (%s)" % self._error['detail']
            self.res_start(status_code, status_phrase, hdrs, dummy)
            self.res_body(body)
            self.res_done(self._error)
            
    def req_body(self, chunk):
        if callable(self.req_body_cb):
            self.req_body_cb(chunk)
        else:
            self._req_body_buffer.append(chunk)
        
    def req_done(self, err=None):
        if callable(self.req_done_cb):
            self.req_done_cb(err)
        else:
            self._req_done = err

    @State('srv_res', 'req_start', '_handle_error')
    def res_start(self, status_code, status_phrase, res_hdrs, res_body_pause):
        "Start a response. Must only be called once per response."
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
            res_top.append("Connection: close")
        res_top.append(linesep)
        self._tcp_conn.write(linesep.join(res_top))
        self._res_state = HEADERS_DONE
        return self.res_body, self.res_done

    @State('srv_res', 'res_start', 'res_body')
    def res_body(self, chunk):
        "Send part of the response body. May be called zero to many times."
        log.debug("%s server res_body %s " % (id(self), len(chunk)))
        assert self._res_state == HEADERS_DONE
        if not chunk: 
            return
        # TODO: if we sent C-L and we've written more than that many bytes, blow up.
        if self._res_delimit == CHUNKED:
            self._tcp_conn.write("%s\r\n%s\r\n" % (hex(len(chunk))[2:], chunk)) # FIXME: why 2:?
        else:
            self._tcp_conn.write("%s" % chunk)

    @State('srv_res', 'res_start', 'res_body')
    def res_done(self, err):
        """
        Signal the end of the response, whether or not there was a body. MUST be
        called exactly once for each response. 
        
        If err is not None, it is an error dictionary (see the error module)
        indicating that an HTTP-specific (i.e., non-application) error occurred
        in the generation of the response; this is useful for debugging.
        """
        log.debug("%s server res_done" % id(self))
        assert self._res_state == HEADERS_DONE
        if err:
            self._tcp_conn.close() # FIXME: need to see if it's a recoverable error...
        elif self._res_delimit == NONE:
            pass
        elif self._res_delimit == CHUNKED:
            self._tcp_conn.write("0\r\n\r\n") # We don't support trailers
        elif self._res_delimit == CLOSE:
            self._tcp_conn.close()
        elif self._res_delimit == COUNTED:
            pass # TODO: double-check the length
        else:
            raise AssertionError, "Unknown response delimiter"
        self.conn_res_done(self)

    @State('srv_res', None, 'req_start')
    def _handle_error(self, err, detail=None):
        "Handle a problem with the request by generating an appropriate response."
        assert self._res_state == WAITING
        if detail:
            err['detail'] = detail
        self._error = err

    
def test_handler(method, uri, hdrs, res_start, req_pause):
    """
    An extremely simple (and limited) server request_handler.
    """
    code = "200"
    phrase = "OK"
    res_hdrs = [('Content-Type', 'text/plain')]
    res_body, res_done = res_start(code, phrase, res_hdrs, dummy)
    res_body('foo!')
    res_done(None)
    return dummy, dummy
    
if __name__ == "__main__":
    sys.stderr.write("PID: %s\n" % os.getpid())
    h, p = sys.argv[1], int(sys.argv[2])
    server = Server(h, p, test_handler)
    push_tcp.run()
