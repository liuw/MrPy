#!/usr/bin/env python
# -*- coding: utf-8 -*-

import Config
from SimpleHTTPServer import SimpleHTTPRequestHandler
from BaseHTTPServer import HTTPServer

def httpd(port):
    server_address = ('', port)

    httpd = HTTPServer(server_address, SimpleHTTPRequestHandler)

    sa = httpd.socket.getsockname()
    print 'Serving HTTP on', sa[0], 'port', sa[1], '...'
    httpd.serve_forever()
