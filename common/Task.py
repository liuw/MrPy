#!/usr/bin/env python
# -*- coding: utf-8 -*-

import Config

MAPPER  = "m"
REDUCER = "r"
MAX_RETRY = Config.MAX_TASK_RETRY

class Task:
    def __init__(self, tid, fl=None, exe=None, type=None, format=None):
        """
        Arguments:
        - `tid`: task id
        - `fl`: input files
        - `exe`: executable
        - `type`: mapper or reducer
        - `format`: (mapper) input / (reducer) output format
        """
        self.tid = tid
        self.fl = fl
        self.exe = exe
        self.type = type
        self.format = format

        self.slave = None
        self.retry = 0
        self.master = None

    # Currently `master` field is not set by the task sender, but we
    # can do this in the future to get sanity check.
    def set_master(self, m):
        self.master = m

    def __str__(self):
        return 'id: %s fl: %s exe: %s type: %s format: %s master: %s'% (self.tid, self.fl, self.exe, self.type, self.format, self.master)
