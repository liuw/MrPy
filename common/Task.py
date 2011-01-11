#!/usr/bin/env python
# -*- coding: utf-8 -*-

MAPPER  = "m"
REDUCER = "r"
MAX_RETRY = 5

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

        # which slave is this task assigned to?
        self.slave = None
        self.retry = 0

    def __str__(self):
        return 'id: %s fl: %s exe: %s type: %s format: %s'% (self.tid, self.fl, self.exe, self.type, self.format)
