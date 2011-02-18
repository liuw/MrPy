#!/usr/bin/env python
# -*- coding: utf-8 -*-

from threading import Thread, Lock
from collections import deque
import socket
import cPickle
import sys

import Config
import Task
import Protocol
import Pool

class Job:
    def __init__(self, slaves, mapper, reducer, M, R, indir,
                 name, informat=None, outformat=None):
        self.slaves = slaves
        self.mapper = mapper
        self.reducer = reducer
        self.M = M
        self.R = R
        self.indir = indir
        self.informat = informat
        self.outformat = outformat
        self.name = name
        self.spool = Pool.SlavePool(slaves)

        self.task_list = deque([])
        self.mapper_list = deque([])
        self.reducer_list = deque([])
        self.running_list = []
        self.completed_list = []
        self.failed_list = []
        self.dead_slaves = []

        self.notify_list = []
        self.notify_list_lock = Lock()

        # intermediate files to reducer mapping
        self._intermediate_to_reducer = {}
        # reducer to intermediate files mapping
        self._reducer_to_intermediate = {}
        self._reducer_to_intermediate2 = {}

    def split(self):
        """Split job into tasks.
        """
        # Input directory should be placed in SHARED_STORAGE
        import distutils.filelist
        file_list = distutils.filelist.findall(dir=self.indir)
        n = len(file_list)
        if n < self.M:
            self.M = n
        files_per_mapper = int(n / self.M)
        remains = n % files_per_mapper

        # Generate mapper list
        pos_start = 0
        # Task ID starts from 1.
        for tid in xrange(1, self.M+1):
            pos_end = tid*files_per_mapper
            tfiles = file_list[pos_start:pos_end]
            pos_start = pos_end
            # Simple algorithm: last mapper takes extra remains
            if tid == self.M:
                tfiles += file_list[pos_start:]
            tsk = Task.Task(tid, self.name, tfiles, self.mapper,
                            Task.MAPPER, self.informat)
            self.mapper_list.append(tsk)

        # Generate recuder list
        files_per_reducer = int(self.M / self.R)
        remains = self.M % self.R
        pos_start = 1
        for tid in xrange(self.M+1, self.M+self.R+1):
            # reducers get their input from different mapper - so-called
            # `shuffle`.

            # NAMING CONVENTION for intermediate files:
            # TID.imf
            pos_end = (tid - self.M) * files_per_reducer
            seq = range(pos_start, pos_end+1)
            if tid == self.M+self.R :
                # last reducer
                seq += range(self.M-remains+1, self.M+1)

            pos_start = pos_end + 1
            rfl = ["%s.imf" % i for i in seq]

            self._reducer_to_intermediate[tid] = set(rfl)
            self._reducer_to_intermediate2[tid] = set(rfl)
            for f in rfl:
                self._intermediate_to_reducer[f] = tid

            tsk = Task.Task(tid, self.name, rfl, self.reducer, Task.REDUCER, format=self.outformat)
            self.reducer_list.append(tsk)

        self.task_list += self.mapper_list
        self.task_list += self.reducer_list

    def _slaves_checker(self):
        import urllib
        import time
        while True:
            for i in xrange(0,len(self.slaves)):
                try:
                    urllib.urlopen('http://'+self.slaves[i]+str(Config.SLAVE_DPORT)+'/notex', timeout=0.5)
                except:
                    # FIXME: slave is dead
                    pass
            time.sleep(20)

    def _state_printer(self):
        import time
        while True:
            print self.completed_list
            print self.failed_list

            time.sleep(10)

    def schedule(self):
        def _distribute(slave, parameter):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect((slave, Config.SLAVE_CPORT))
                s.send(parameter)
            except:
                print >>sys.stderr, "Failed to send task to", slave

        while True:
            try:
                # N.B. order matters, see exception handler below
                s = self.spool.get()
                t = self.task_list.popleft()

                self.running_list.append(t)
                t.slave = s
                t.state = Task.INPROGRESS
                objs = cPickle.dumps(t)
                _distribute(s, objs)
                time.sleep(1)
            except Pool.NoMoreSlaveError:
                # No more slave to assign task
                print >>sys.stderr, 'No more slave to assgin task'
                time.sleep(5)
            except IndexError:
                # No more task to schedule
                print >>sys.stderr, 'No more task to schedule'
                self.spool.put(s)
                time.sleep(5)
            except:
                raise

    def _handle_intermediate_file(self, fn):
        # Notify reducer if all its mappers have finished.
        r = None
        try:
            r = self._intermediate_to_reducer.pop(fn)
        except:
            print >>sys.stderr, 'Bad intermediate filename:', fn
            return
        try:
            files = self._reducer_to_intermediate[r]
        except KeyError:
            print >>sys.stderr, 'Bad reducer:', r
            return
        files.pop(fn)
        if len(files) == 0:
            self._reducer_to_intermediate.pop(r)
            self.notify_list_lock.acquire()
            self.notify_list.append(r)
            self.notify_list_lock.release()

    def notifier(self):
        """This thread is used to notify reducers."""
        self.notify_list_lock.acquire()
        for r in self.notify_list:
            # reducer may be not scheduled!
            if r.state == Task.IDLE:
                # how to schedule?
                # if we go directly to schedule and there is no slave
                # available, master will block forever - since it can
                # not process "put slave" message.
                # so we just skip this reducer and wait.
                pass
            elif r.state == Task.INPROGRESS:
                # FIXME: collect all (mapper, file) pair and send it to reducer.
                files = self._reducer_to_intermediate2[r]
                l = []
                for f in files:
                    # if we are here, all mappers must have completed.
                    # so we just scan mapper_list
                    idx = int(f[:-4]) - 1 # strip .imf and convert to index
                    tsk = self.mapper_list[idx]
                    s = tsk.slave
                    fullpath = 'http://'+str(s)+':'+str(Config.SLAVE_DPORT)+'/'+f
                    l.append(fullpath)
                msg = Protocol.MasterMsg(r.tid, Protocol.FILE_READY, l)
                objs = cPickle.dumps(msg)
                slave = r.slave
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    s.connect((slave, Config.SLAVE_CPORT))
                    s.send(objs)
                except:
                    print >>sys.stderr, "Failed to notify reducer %s on %s" % (r, slave)
            else:
                print >>sys.stderr, 'Trying to notify a reducer in state %s' % (r.state,)
        self.notify_list_lock.release()
        time.sleep(10)

    def _handle_message(self, msg):
        # When we are here, it means that one task has finished, no
        # matter it failed or completed.
        # So we need to schedule other task if any.
        # Only one thread manipulate lists, so no lock is necessary
        tsk_tid = msg.tid
        slave_state = msg.state
        if msg.extra:
            extra = msg.extra
        # FIXME: add handling logic, shedule new task if any.
        idx = -1
        for i in xrange(len(self.running_list)):
            if self.running_list[i].tid == tsk_tid:
                idx = i
                break
        if idx == -1:
            print >>sys.stderr, 'Unable to find task %s' % (tsk_tid,)
            return
        tsk = self.running_list.pop(idx)
        tsk.state = Task.COMPLETED
        slave = tsk.slave
        # put slave
        self.spool.put(slave)

        if slave_state == Protocol.TASK_FAILED:
            self.failed_list.append(tsk)
            if tsk.type == Task.MAPPER:
                print >>sys.stderr, "Mapper %s failed" % (tsk.tid,)
                # gather task information so that we can notify mapper
                # at the right time.
                # a falied mapper will have a empty intermediate file
                # anyway.
                ifn = str(tsk.tid)+'.imf'
                self._handle_intermediate_file(ifn)
            else:
                print >>sys.stderr, "Reducer %s failed" % (tsk.tid,)
        elif (slave_state == Protocol.TASK_COMPLETED):
            self.completed_list.append(tsk)
            if tsk.type == Task.MAPPER:
                print >>sys.stderr, "Mapper %s completed" % (tsk.tid,)
                # gather information
            else:
                print >>sys.stderr, "Reducer %s completed" % (tsk.tid,)
        else:
            print >>sys.stderr, 'Unkown slave state %s' % (slave_state,)

    def _msgd(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', Config.MASTER_MPORT))
        s.listen(10)
        OBJ_MAX_SIZE = 1000
        while True:
            c, addr = s.accept()
            try:
                objs = c.recv(OBJ_MAX_SIZE)
                msg = cPickle.loads(objs)
                assert(type(msg) == type(Protocol.SlaveMsg))
                self._handle_message(msg)
            except:
                print >>sys.stderr, 'Unable to handle message %s' % (msg,)

    def run(self):

        self.split()

        chkt = Thread(target=self._slaves_checker)
        chkt.setDaemon(True)
        chkt.start()
        spt = Thread(target=self._state_printer)
        spt.setDaemon(True)
        spt.start()
        msgd = Thread(target=self._msgd)
        msgd.setDaemon(True)
        msgd.start()
        scheder = Thread(target=self.schedule)
        scheder.setDaemon(True)
        scheder.start()
        notifier = Thread(target=self.notifier)
        notifier.setDaemon(True)
        notifier.start()

        chkt.join()
        spt.join()
        msgd.join()
        scheder.join()

if __name__ == '__main__':
    j = Job(['1', '2', '3'], 'ls', 'cat', 1, 1, '/home/liuw/pymr', 'test')
    j.run()
