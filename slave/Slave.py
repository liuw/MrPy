#!/usr/bin/env python
# -*- coding: utf-8 -*-

import socket
import sys
import cPickle
import threading
import time
import urllib
from subprocess import Popen, PIPE

import Config
import Httpd
from Task import Task
from Protocol import *

OBJ_MAX_SIZE = 1000

def handle_task(task, master, sock):
    # We don't fork here, so main thread will block until we are done
    # with this task. But if we want to improve system throughput, it
    # is better to fork here.
    exe = task.exe
    task_type = task.type
    tid = task.tid
    fl = task.fl
    format = task.format
    task.master = master
    retrieve = open

    if task_type == Task.REDUCER:
        # we should block and wait for notification
        c, addr = sock.accept()
        if addr != master:
            raise Exception("Bad msg?")
        objs = c.recv(OBJ_MAX_SIZE)
        mmsg = cPickle.dumps(objs)
        if type(mmsg) != type(MasterMsg):
            raise Exception("Bad msg: not master msg")
        fl = mmsg.fl
        retrieve = urllib.urlopen

    # first, gather all contents
    content = []
    for f in fl:
        print "Retrieving ", f
        try:
            content += retrieve(f).read()
        except IOError:
            # task.retry += 1
            # time.sleep(2)
            raise

    # run exe in streaming mode

    # if task is reduce task, format means output format
    # if task is map task, format means input format

    # INVOKE EXECUTABLE AND FEED IT / MILK IT
    try:
        p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE,
                  close_fds=True)
        out, err = p.communicate(input=content)
    except OSError, e:
        print e

    if len(err) != 0:
        print >>sys.stderr, '***', str(tid)
        print >>sys.stderr, err

    slavemsg = SlaveMsg(self.tid, Protocol.TASK_COMPLETED)
    if task_type == Task.MAPPER:
        # FIXME: use format
        outfn = str(task.pid) + '.imf'
        # N.B. create intermediate file first
        outf = open(outfn, 'w')
        outf.close()
        try:
            outf = open(outfn, 'w')
            outf.write(out)
            outf.close()
            slavemsg = SlaveMsg(self.tid, Protocol.TASK_COMPLETED)
        except IOError, e:
            print >>sys.stderr, 'Write intermediate file error'
            slavemsg = SlaveMsg(self.tid, Protocol.TASK_FAILED, e)
    else:
        outfn = Config.SHARED_STORAGE+'/'+jobname+'/'+str(task.pid)+'.out'
        try:
            outf = open(outfn)
            outf.write(out)
            outf.close()
            slavemsg = SlaveMsg(self.tid, Protocol.TASK_COMPLETED)
        except IOError, e:
            print >>sys.stderr, 'Write output file error'
            slavemsg = SlaveMsg(self.tid, Protocol.TASK_FAILED, e)

    # notify master
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((task.master, Config.MASTER_MPORT))
        s.send(cPickle.dumps(slavemsg))
    except IOError, e:
        print e

def main():
    http_thread = threading.Thread(target=Httpd.httpd,args=(Config.SLAVE_DPORT,))
    http_thread.setDaemon(True)
    http_thread.start()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('0.0.0.0', Config.SLAVE_CPORT))
    s.listen(10)

    while True:
        print >>sys.stderr, "Start to serve..."
        c, addr = s.accept()
        try:
            objs = c.recv(OBJ_MAX_SIZE)
            tsk = cPickle.loads(objs)
            print >>sys.stderr, "Received from %s:\n  %s" % (addr, obj)
            handle_task(tsk, addr, s)
        except:
            exit(-1)

    httpd_thread.join()

if __name__ == '__main__':
    main()
