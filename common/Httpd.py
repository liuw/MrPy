#!/usr/bin/env python
# -*- coding: utf-8 -*-

import Config
from SimpleHTTPServer import SimpleHTTPRequestHandler
from BaseHTTPServer import HTTPServer

server_address = ('', Config.SLAVE_DPORT)

httpd = HTTPServer(server_address, SimpleHTTPRequestHandler)

sa = httpd.socket.getsockname()
print 'Serving HTTP on', sa[0], 'port', sa[1], '...'
httpd.serve_forever()
