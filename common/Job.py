#!/usr/bin/env python
# -*- coding: utf-8 -*-

import Config
import Task

class Job:
    def __init__(self, mapper, reducer, M, R, indir,
                 outdir, informat=None, outformat=None):
        """
        Arguments:
        - `mapper`: mapper program
        - `reducer`: reducer program
        - `M`: number of mappers
        - `R`: number of reducers
        - `indir`: directory for input files
        - `outdir`: directory for output files
        - `informat`: input format for mapper
        - `outformat`: output format for reducer
        """
        self.mapper = mapper
        self.reducer = reducer
        self.M = M
        self.R = R
        self.indir = indir
        self.outdir = outdir
        self.informat = informat
        self.outformat = outformat

        self.task_list = []
        self.mapper_list = []
        self.reducer_list = []
        self.completed_list = []
        self.failed_list = []

        # intermediate files to reducer mapping
        self._intermediate_to_reducer = {}


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
            tsk = Task.Task(tid, tfiles, self.mapper,
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
            rfl = [self.outdir+"/%s.imf" % i for i in seq]
            for f in rfl:
                self._intermediate_to_reducer[f] = tid

            tsk = Task.Task(tid, rfl, self.reducer, Task.REDUCER,
                            format=self.outformat)
            self.reducer_list.append(tsk)

        self.task_list = self.mapper_list + self.reducer_list
