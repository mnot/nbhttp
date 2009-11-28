#!/usr/bin/env python

"""
c_zlib - zlib for Python using ctypes.

This is a quick and nasty implementation of zlib using Python ctypes, in order to expose the ability
to set a compression dictionary (which isn't available in the zlib module).
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

    un = "\x00\x06accept\x00Zapplication/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5\x00\x0eaccept-charset\x00\x1eISO-8859-1,utf-8;q=0.7,*;q=0.3\x00\x0faccept-encoding\x00\x0cgzip,deflate\x00\x0faccept-language\x00\x0een-US,en;q=0.8\x00\x06method\x00\x03GET\x00\x03url\x00\x1bhttp://192.168.1.6:8000/foo\x00\nuser-agent\x00|Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_2; en-US) AppleWebKit/532.5 (KHTML, like Gecko) Chrome/4.0.256.0 Safari/532.5\x00\x07version\x00\x08HTTP/1.1"

    dictionary = \
"optionsgetheadpostputdeletetraceacceptaccept-charsetaccept-encodingaccept-" \
"languageauthorizationexpectfromhostif-modified-sinceif-matchif-none-matchi" \
"f-rangeif-unmodifiedsincemax-forwardsproxy-authorizationrangerefererteuser" \
"-agent10010120020120220320420520630030130230330430530630740040140240340440" \
"5406407408409410411412413414415416417500501502503504505accept-rangesageeta" \
"glocationproxy-authenticatepublicretry-afterservervarywarningwww-authentic" \
"ateallowcontent-basecontent-encodingcache-controlconnectiondatetrailertran" \
"sfer-encodingupgradeviawarningcontent-languagecontent-lengthcontent-locati" \
"oncontent-md5content-rangecontent-typeetagexpireslast-modifiedset-cookieMo" \
"ndayTuesdayWednesdayThursdayFridaySaturdaySundayJanFebMarAprMayJunJulAugSe" \
"pOctNovDecchunkedtext/htmlimage/pngimage/jpgimage/gifapplication/xmlapplic" \
"ation/xhtmltext/plainpublicmax-agecharset=iso-8859-1utf-8gzipdeflateHTTP/1" \
".1statusversionurl"

    s = "x\xbb\xdf\xa2Q\xb2dTMO\xc2@\x10\x9d\x84\x04\x89\x07\x08\x17\x8ff\x8e\x80\xe3n\xbbP\xf9h<\x18b\x80`\xc3\xa1\x10\x8d\x17R\xcb\x9an\xa8\x14\xa5'\x13\xff\xbb\xdbm)\x11\xb3\xb7\x97\xd9\xbc\xf7f\xe6\r\xd4\xa0\x9a\xef\x0b\xbc\x9e\xb1\xd3?\xfa\x9b\x0c-}\xb8\x9f\xf7\x16\x1b\xd2I\x93\x01\x06T\xfa\xa3\x0e\xef\x18\xcc\x81\xfa\xdf0\xc3\xf5\xcc_\x14R\xc9h5u}\xca\xeb\xbb\xd08\x8b;433T\xb8\xa1\xc3&\x8c\xca\x9a\xe3\x86@]\xeenW>\xc9B\x08T?\xf4\x81I6P\x99<.\xa1\x92\xb9\xbd\x8a\xd2t?\xe2\xdc\x16}f\xe9g\x8f\x06:\xa6\x1c.O\xb9\x85\x1f/\xf9Vq\x1cp\x87Y\xd8\xf2\x82P\xed\xd2\xe4\x10\xb9\xb8rq\xa6\xb7)F\x8d\xe1\xc2\xc7\x17\xb4\xad\xf5\xddZ\xb8hx\xdb\xf8\xa0\xfb%\x9f\xe5\xdb\\\xa5\xdc\xe9\n\xe6`k>]zO\x84\xb1\xdaJ\x9c\xc8p\x9b\xb4q\x1c\xe9\xc3$yO\xd3\x8b\xde\x90\xd9\x02\xfd\xe0=\xf8R\xf9\x17\xb8(\x86\x03\xb5\xe3\xcc~\x01\x00\x00\xff\xff"
    s = 'x\xbb\xdf\xa2Q\xb2dTMk\xc2@\x10\x1d\x10\xac\x94b\xcf\xbd\x949\xaa\x9d\xee&\xab\xa9\x1f\xa1\x87"b\xc5\x06\x0fQZz\x91\xd4\xaed1u\xfb\x91S\xa1\xff\xbd\x9bM\x8c\xa8\xec\xed\xed,\xf3\xde\xce{\x035\xa8\xe6~\x81\xd7\xa3\xeet\xd2\xfe&CK\x1d\xfe\xd7\xbd\xc3\xfa\xb4\xe7d\x81\x1e\x95\xfa\xa8\xc5[\x16\xf3\xa0~\x18f\xb8\x9e\x84\xb3\x82*Y\xae\xb6\xaeKy}\x1b.\x8f\xe2\x0e\x17\x99\x18*\xd4\x94\xd7;s@]no\x17!\xc9\x82\x03T?\xccn\xd1\xefP\x19\x8f\xe6P\xc9\x84^\xc5i\xfa9\xe0\xdc\x15]\xe6\x98\xe3\x0ez&\xa1|\xad5\x9c\xefS\x0b\x7f\x81\xfeUI\x12q\x8f9\xd8\x08\xa2\x95\xda\xa6\xfa\'\xf6q\xe1\xe3\xc4x)A\x83\xe1,\xc4\x17t\x9d\xe5\xddR\xf8h[7\xf1\xc1\xfc\x96|\x96oS\x95r\xaf-\x98\x87\x8d\xe9\xe3<x"L\xd4F\xe2X\xae6\xba\x89\xc3\xd8\xac%\xc9;\x86\x81\xe8\xf4\x99+0\x8c\xd6\xd1\xb7\xca\x9f\xc0Y1\x1a\xa8\xed&\xf6\x0f\x00\x00\xff\xff'

#    print repr(compress(un, -1, dictionary)[:25])
#    print repr(s[:25])
#    print decompress(s, dictionary)