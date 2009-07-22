#!/usr/bin/env python

"""
A simple HTTP proxy as a demonstration.
"""


import sys
try: # run from dist without installation
    sys.path.insert(0, "..")
    from src import Client, Server, header_dict, run, client, schedule
except ImportError:
    from nbhttp import Client, Server, header_dict, run, client, schedule

# TODO: CONNECT support
# TODO: remove headers nominated by Connection
# TODO: add Via

class ProxyClient(Client):
    read_timeout = 10
    connect_timeout = 15

def proxy_handler(method, uri, req_hdrs, s_res_start, req_pause):
    # can modify method, uri, req_hdrs here
    def c_res_start(version, status, phrase, res_hdrs, res_pause):
        # can modify status, phrase, res_hdrs here
        res_body, res_done = s_res_start(status, phrase, res_hdrs, res_pause)
        # can modify res_body here
        return res_body, res_done
    c = ProxyClient(c_res_start)
    req_body, req_done = c.req_start(method, uri, req_hdrs, req_pause)
    # can modify req_body here
    return req_body, req_done 


if __name__ == "__main__":
    import sys
    port = int(sys.argv[1])
    server = Server('', port, proxy_handler)
    run()