#!/usr/bin/env python

import sys

for fn in sys.stdin:
    fn = fn.strip()

    content = open(fn).read()

    for line in content.split('\n'):
        line = line.strip()
        words = line.split()
        i = 0
        hasht = {}
        for word in words:
            # (word, pos)
            try:
                hasht[word] += ['(%s, %s)' % (fn,i)]
            except KeyError:
                hasht[word] = ['(%s, %s)' % (fn,i)]
            i += 1

        for k in hasht:
            print "%s\t" % (k,),
            for i in hasht[k]:
                print "%s\t" % str(i),
            print
