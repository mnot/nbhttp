#!/usr/bin/env python

"""
nbhttp.common - shared HTTP infrastructure

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

import re
lws = re.compile("\r?\n[ \t]+", re.M)
hdr_end = re.compile(r"\r?\n\r?\n", re.M)
linesep = "\r\n" 

# conn_modes
CLOSE, COUNTED, CHUNKED, NONE = 0, 1, 2, 3

# states
WAITING, HEADERS_DONE = 1, 2

idempotent_methods = ['GET', 'HEAD', 'PUT', 'DELETE', 'OPTIONS', 'TRACE']
no_body_status = ['100', '101', '204', '304']
hop_by_hop_hdrs = ['connection', 'keep-alive', 'proxy-authenticate', 
                   'proxy-authorization', 'te', 'trailers', 'transfer-encoding', 
                   'upgrade']


class HttpMessageParser:
    """
    This is a base class for something that has to parse HTTP messages, request
    or response. It expects you to override _input_start, _input_body and 
    _input_end, and call _handle_input when you get bytes from the network.
    """

    def __init__(self):
        self._input_buffer = ""
        self._input_state = WAITING
        self._input_delimit = None
        self._input_body_left = 0

    def _input_start(self, top_line, hdr_tuples, conn_tokens, transfer_codes, content_length):
        # MUST return (float version, boolean allows_body)
        raise NotImplementedError

    def _input_body(self, chunk):
        raise NotImplementedError

    def _input_end(self, complete):
        raise NotImplementedError
    
    def _input_extra(self, chunk):
        raise NotImplementedError
        
    def _handle_input(self, instr):
        if self._input_buffer != "":
            instr = self._input_buffer + instr # will need to move to a list if writev comes around
            self._input_buffer = ""
        if self._input_state == WAITING:
            if hdr_end.search(instr): # found one
                rest = self._parse_headers(instr)
                self._handle_input(rest)
            else: # partial headers; store it and wait for more
                self._input_buffer = instr
        elif self._input_state == HEADERS_DONE:
            if self._input_delimit == NONE: # a message without a body
                self._input_end(True)
                self._input_state = WAITING
                if instr: # FIXME: will not work with pipelining
                    self._input_extra(instr)
            elif self._input_delimit == CLOSE:
                self._input_body(instr)
            elif self._input_delimit == CHUNKED:
                if self._input_body_left > 0:
                    if self._input_body_left < len(instr): # got more than the chunk
                        this_chunk = self._input_body_left
                        self._input_body(instr[:this_chunk])
                        self._input_body_left = -1
                        self._handle_input(instr[this_chunk+2:]) # +2 consumes the CRLF
                    elif self._input_body_left == len(instr): # got the whole chunk exactly
                        self._input_body(instr)
                        self._input_body_left = -1
                    else: # got partial chunk
                        self._input_body(instr)
                        self._input_body_left -= len(instr)
                elif self._input_body_left == 0: # done
                    if len(instr) >= 2 and instr[:2] == linesep:
                        self._input_end(True)
                        self._input_state = WAITING
                        self._handle_input(instr[2:])
                    elif hdr_end.search(instr):
                        self._input_end(True)
                        self._input_state = WAITING
                        trailers, rest = hdr_end.split(instr, 1) # TODO: process trailers
                        self._handle_input(rest)
                    else:
                        self._input_buffer = instr
                else: # new chunk
                    try:
                        # they really need to use CRLF
                        chunk_size, rest = instr.split("\r\n", 1)
                    except ValueError:
                        # got a CRLF without anything behind it.. wait.
                        self._input_buffer += instr
                        return
                    if chunk_size.strip() == "": # ignore bare lines
                        return self._handle_input(rest)
                    if ";" in chunk_size: # ignore chunk extensions
                        chunk_size = chunk_size.split(";", 1)[0]
                    self._input_body_left = int(chunk_size, 16) # TODO: handle bad chunks
                    self._handle_input(rest)
            elif self._input_delimit == COUNTED:
                assert self._input_body_left >= 0, \
                    "message counting problem (%s)" % self._input_body_left
                # process body
                if self._input_body_left <= len(instr): # got it all (and more?)
                    self._input_body(instr[:self._input_body_left])
                    self._input_state = WAITING
                    if instr[self._input_body_left:]:
                        # This will catch extra input that isn't on packet boundaries.
                        self._input_extra(instr[self._input_body_left:])
                    else:
                        self._input_end(True) # 
                else: # got some of it
                    self._input_body(instr)
                    self._input_body_left -= len(instr)
            else:
                raise Exception, "Unknown input delimiter %s" % self._input_delimit
        else:
            raise Exception, "Unknown state %s" % self._input_state

    def _parse_headers(self, instr):
        top, rest = hdr_end.split(instr, 1)
        hdr_lines = lws.sub(" ", top).splitlines()
        try:
            top_line = hdr_lines.pop(0)
        except IndexError: # empty
            return ""
        hdr_tuples = []
        conn_tokens = []
        transfer_codes = []
        content_length = None
        for line in hdr_lines:
            try:
                fn, fv = line.split(":", 1)
                hdr_tuples.append((fn, fv))
            except ValueError, why:
                raise # FIXME: bad header
            f_name = fn.strip().lower()
            f_val = fv.strip()

            # parse connection-related headers
            if f_name == "connection":
                conn_tokens += [v.strip().lower() for v in f_val.split(',')]
            elif f_name == "transfer-encoding":
                transfer_codes += [v.strip().lower() for v in f_val.split(',')]
            elif f_name == "content-length":
                try:
                    content_length = int(f_val) # FIXME: barf on more than one C-L
                except ValueError, why:
                    pass

        # ignore content-length if transfer-encoding is present
        if transfer_codes != [] and content_length != None:
#            hdr_tuples = [(n,v) for (n,v) in hdr_tuples if n.strip().lower() != 'content-length']
            content_length = None 

        try:
            version, allows_body = self._input_start(top_line, hdr_tuples, 
                                    conn_tokens, transfer_codes, content_length)
        except ValueError: # parsing error of some kind; abort.
            return ""
                                
        self._input_state = HEADERS_DONE
        if not allows_body:
            self._input_delimit = NONE
        else:
            if len(transfer_codes) > 0:
                if 'chunked' in transfer_codes:
                    self._input_delimit = CHUNKED
                    self._input_body_left = -1 # flag that we don't know
                else:
                    self._input_delimit = CLOSE # FYI, doesn't make sense for requests
            elif content_length != None:
                self._input_delimit = COUNTED
                self._input_body_left = content_length
            elif 'close' in conn_tokens: # FIXME: this doesn't make sense for requests
                self._input_delimit = CLOSE
            else: # assume 0 length body
                self._input_delimit = COUNTED
                self._input_body_left = 0
        return rest
    
def dummy(*args, **kw):
    pass
