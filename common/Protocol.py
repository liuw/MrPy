#!/usr/bin/env python
# -*- coding: utf-8 -*-

TASK_FAILED    = "task_failed"
TASK_COMPLETED = "task_completed"

class SlaveMsg:
    def __init__(self, tid, state, extra=None):
        self.tid = tid
        self.state = state
        self.extra = extra

    def __str__(self):
        s = "SM: TID %s STATE %s" % (self.tid, self.state)
        if self.extra:
            s += " EXTRA %s" % (self.extra,)

        return s


FILE_READY = "file_ready"

class MasterMsg:
    def __init__(self, tid, cmd, fl=None):
        self.tid = tid
        self.cmd = cmd
        self.fl = fl

    def __str__(self):
        s = "MM: TID %s STATE %s FL: %s" % (self.tid, self.state, self.fl)
        return s
