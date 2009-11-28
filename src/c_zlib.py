#!/usr/bin/env python

"""
c_zlib - zlib for Python using ctypes.

This is a quick and nasty implementation of zlib using Python ctypes, in order
to expose the ability to set a compression dictionary (which isn't available
in the zlib module).
"""

__license__ = """
Copyright (c) 2009 Mark Nottingham <mnot@pobox.com>
 
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
 
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
 
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""


import ctypes as C
from ctypes import util

_zlib = C.cdll.LoadLibrary(util.find_library('libz'))

class _z_stream(C.Structure):
    _fields_ = [
        ("next_in", C.POINTER(C.c_ubyte)),
        ("avail_in", C.c_uint),
        ("total_in", C.c_ulong),
        ("next_out", C.POINTER(C.c_ubyte)),
        ("avail_out", C.c_uint),
        ("total_out", C.c_ulong),
        ("msg", C.c_char_p),
        ("state", C.c_void_p),
        ("zalloc", C.c_void_p),
        ("zfree", C.c_void_p),
        ("opaque", C.c_void_p),
        ("data_type", C.c_int),
        ("adler", C.c_ulong),
        ("reserved", C.c_ulong),
        
    ]

# TODO: get zlib version with ctypes
ZLIB_VERSION = C.c_char_p("1.2.3")
Z_NULL = 0x00
Z_OK = 0x00
Z_STREAM_END = 0x01
Z_NEED_DICT = 0x02
Z_BUF_ERR = -0x05

Z_NO_FLUSH = 0x00
Z_SYNC_FLUSH = 0x02
Z_FINISH = 0x04

CHUNK = 1024 * 32

class Compressor:
    def __init__(self, level=-1, dictionary=None):
        self.level = level
        self.dictionary = dictionary
        self.st = _z_stream()
        err = _zlib.deflateInit_(C.byref(self.st), self.level, ZLIB_VERSION, C.sizeof(self.st))
        assert err == Z_OK, err

    def __call__(self, input):
        out = []
        self.st.avail_in  = len(input)
        self.st.next_in   = C.cast(C.c_char_p(input), C.POINTER(C.c_ubyte))
        self.st.avail_out = Z_NULL
        self.st.next_out = C.cast(Z_NULL, C.POINTER(C.c_ubyte))
        if self.dictionary:
            err = _zlib.deflateSetDictionary(
                C.byref(self.st),
                C.cast(C.c_char_p(self.dictionary), C.POINTER(C.c_ubyte)),
                len(self.dictionary)
            )
            assert err == Z_OK, err
        while True:
            self.st.avail_out = CHUNK
            outbuf = C.create_string_buffer(CHUNK)
            self.st.next_out = C.cast(outbuf, C.POINTER(C.c_ubyte))
            err = _zlib.deflate(C.byref(self.st), Z_FINISH)
            out.append(outbuf[:CHUNK-self.st.avail_out])
            if err == Z_STREAM_END: break
            elif err in [Z_OK, Z_BUF_ERR]: pass
            else:
                raise AssertionError, err
        return "".join(out)

    def __del__(self):
        err = _zlib.deflateEnd(C.byref(self.st))
        assert err == Z_OK, err

    
class Decompressor:
    def __init__(self, dictionary=None):
        self.dictionary = dictionary
        self.st = _z_stream()
        err = _zlib.inflateInit2_(C.byref(self.st), 15, ZLIB_VERSION, C.sizeof(self.st))
        assert err == Z_OK, err

    def __call__(self, input):
        out = []
        self.st.avail_in  = len(input)
        self.st.next_in   = C.cast(C.c_char_p(input), C.POINTER(C.c_ubyte))
        self.st.avail_out = Z_NULL
        self.st.next_out = C.cast(Z_NULL, C.POINTER(C.c_ubyte))
        while True:
            self.st.avail_out = CHUNK
            outbuf = C.create_string_buffer(CHUNK)
            self.st.next_out = C.cast(outbuf, C.POINTER(C.c_ubyte))
            err = _zlib.inflate(C.byref(self.st), Z_SYNC_FLUSH)
            if err == Z_NEED_DICT:
                assert self.dictionary, "no dictionary provided"
                dict_id = _zlib.adler32(
                    0L,
                    C.cast(C.c_char_p(self.dictionary), C.POINTER(C.c_ubyte)),
                    len(self.dictionary)
                )
    #            assert dict_id == st.adler, 'incorrect dictionary (%s != %s)' % (dict_id, st.adler)
                err = _zlib.inflateSetDictionary(
                    C.byref(self.st),
                    C.cast(C.c_char_p(self.dictionary), C.POINTER(C.c_ubyte)),
                    len(self.dictionary)
                )
                assert err == Z_OK, err
            elif err in [Z_OK, Z_STREAM_END]:
                out.append(outbuf[:CHUNK-self.st.avail_out])
            elif err == Z_BUF_ERR:
                pass
            else:
                raise AssertionError, err
            if err == Z_STREAM_END:
                break
        return "".join(out)

    def __del__(self):
        err = _zlib.inflateEnd(C.byref(self.st))
        assert err == Z_OK, err
    
    
def _test(input=None):
    import zlib, time, sys
    input = open(sys.argv[1]).read()
    
    # compression tests
    start = time.time()
    ct_archive = Compressor(6)(input)
    print "ctypes zlib compress: %2.2f seconds" % (time.time() - start)
    start = time.time()
    zl_archive = zlib.compress(input, 6)
    print "zlib module compress: %2.2f seconds" % (time.time() - start)
    assert ct_archive == zl_archive, "%s != %s" % (len(ct_archive), len(zl_archive))
    # decompressions tests
    start = time.time()
    ct_orig = Decompressor()(ct_archive)
    print "ctypes zlib decompress: %2.2f seconds" % (time.time() - start)
    start = time.time()
    zl_orig = zlib.decompress(ct_archive)
    print "zlib module decompress: %2.2f seconds" % (time.time() - start)
    assert ct_orig == zl_orig, "%s != %s" % (len(ct_orig), len(zl_orig))
    # dictionary compression
    dictionary = "this is the test string to see if dictionaries work. Please bear with us."
    di_archive = Compressor(6, dictionary)(dictionary)
    no_di_archive = Compressor(6)(dictionary)
    di_orig = Decompressor(dictionary)(di_archive)
    assert dictionary == di_orig, "%s != %s" % (len(dictionary), len(di_orig))
    print "%s bytes with perfect dictionary; %s without; %s uncompressed." % (
        len(di_archive), len(no_di_archive), len(dictionary))
    print "done."

    
if __name__ == '__main__':
    _test()
