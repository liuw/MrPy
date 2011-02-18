#!/usr/bin/env python
# -*- coding: utf-8 -*-

import BaseHTTPServer
import cPickle
import Master

import Config
import Protocol

class MasterHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def  do_GET(self):
        objs = self.path[1:] # strip `/`, next is object in string
        msg = cPickle.loads(objs)
        # FIXME: Here we handle message and return response
        Master.HandleMessage(msg)

        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.end_headers()
        self.wfile.write('MSG RCVD')

def master_msgd(slaves):
    slaves_list = slaves
    server_address = ('', Config.MASTER_MPORT)
    httpd = BaseHTTPServer.HTTPServer(server_address, MasterHandler)
    httpd.serve_forever()

class Daemon:
    def __init__(self, daemonized=False):
        logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
        self.port = port
        self.daemonized = daemonized

    def daemonize(self):
        import os
        if os.fork():
            os._exit(0)
        else:
            os.setsid()

        os.close(0)
        os.close(1)
        os.close(2)
        os.open('/dev/null', os.O_RDWR)
        os.dup(0)
        os.dup(0)

    def run(self):
        if self.daemonized:
            self.daemonize()
