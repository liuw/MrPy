#!/usr/bin/env python
# -*- coding: utf-8 -*-

import Config

# Task types
MAPPER  = "m"
REDUCER = "r"

# Task max retry
MAX_RETRY = Config.MAX_TASK_RETRY

# Task states
IDLE       = "idle"
INPROGRESS = "inprogress"
COMPLETED  = "completed"
FAILED     = "failed"

class Task:
    def __init__(self, tid, jobname, fl=None, exe=None, type=None, format=None):
        self.tid = tid
        self.jobname = jobname
        self.fl = fl
        self.exe = exe
        self.type = type
        self.format = format

        self.slave = None
        self.retry = 0
        self.master = None
        self.state = IDLE

    def __str__(self):
        return 'id: %s jobname: %s fl: %s exe: %s type: %s format: %s master: %s slave: %s'% (self.tid, self.jobname, self.fl, self.exe, self.type, self.format, self.master, self.slave)
