#!/usr/bin/env python

from operator import itemgetter
import sys

hasht = {}

for line in sys.stdin:
    line = line.strip()
    idx = line.index('\t')
    word = line[:idx]
    poslist = line[idx+1:]
    try:
        hasht[word] += poslist
    except KeyError:
        hasht[word] = poslist

for k in hasht:
    print "%s\t%s" % (k, hasht[k])
