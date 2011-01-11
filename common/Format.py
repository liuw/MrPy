#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Use class or constant?
# If use class, it should act like a filter.
# If use constant, this filter job is delegated to slave.
class Format:
    def filter(self):
        pass

class KeyValueTextInputFormat(Format):
    def filter(self, fn, pos, line):
        return "%s@%s\t%s" % (fn, pos, line)

class RawTextLineInputFormat(Format):
    def filter(self, fn, pos, line):
        return "%s" % (line,)
