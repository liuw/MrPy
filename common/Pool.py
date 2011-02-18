#!/usr/bin/env python

class NoMoreSlaveError(Exception):
    pass

class SlavePool:
    def __init__(self, l):
        self.pool  = set(l)

    def get(self):
        try:
            e = self.pool.pop()
        except KeyError:
            raise NoMoreSlaveError
        return e

    def put(self, e):
        self.pool.add(e)

    def get_pool(self):
        return self.pool.copy()
