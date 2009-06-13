
from client import Client
from server import Server
from push_tcp import run, stop, schedule

def header_dict(header_tuple, strip=None):
    if strip == None:
        strip = []
    return dict([(n.strip().lower(), v.strip()) for (n,v) in header_tuple])

def dummy(*args, **kw):
    pass