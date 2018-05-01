#! /usr/bin/env python

import os
import shutil
import time

import cdms2
import cwt
import mock
import psutil
import vcs
from django import test

from wps import settings
from wps.tasks import process

vcs.download_sample_data_files()

class Benchmark(object):
    def __init__(self):
        self.io_start = None
        self.mem_start = None
        self.time_start = None

        self.io_stop = None
        self.mem_stop = None
        self.time_stop = None

        self.proc = psutil.Process()

    def __enter__(self):
        self.io_start = self.proc.io_counters()

        self.mem_start = self.proc.memory_info()

        self.time_start = time.time()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.io_stop = self.proc.io_counters()

        self.mem_stop = self.proc.memory_info()

        self.time_stop = time.time()

    def stats(self):
        print 'PID: ', self.proc.pid
        print 'Cmdline: ', self.proc.cmdline()
        print 'CPU times: ', self.proc.cpu_times()
        print 'CPU num: ', self.proc.cpu_num()
        print 'Time Elapsed:', str((self.time_stop - self.time_start) * 1000.0), 'ms'
        print 'Start IO', self.io_start
        print 'Stop IO', self.io_stop
        print 'Start Memory', self.mem_start
        print 'Stop Memory', self.mem_stop

class BenchmarkTestCase(test.TestCase):
    def setUp(self):
        agg_files = [
            # (12, 1, 32, 64) 1979-1-16 12:0:0.0 1979-12-16 12:0:0.0
            cwt.Variable(os.path.join(vcs.sample_data, 'tas_ccsr-95a_1979.01-1979.12.nc'), 'tas'),
            cwt.Variable(os.path.join(vcs.sample_data, 'tas_ccsr-95a_1980.01-1980.12.nc'), 'tas'),
            cwt.Variable(os.path.join(vcs.sample_data, 'tas_ccsr-95a_1981.01-1981.12.nc'), 'tas'),
            cwt.Variable(os.path.join(vcs.sample_data, 'tas_ccsr-95a_1982.01-1982.12.nc'), 'tas'),
            cwt.Variable(os.path.join(vcs.sample_data, 'tas_ccsr-95a_1983.01-1983.12.nc'), 'tas'),
            # (12, 1, 32, 64) 1984-1-16 12:0:0.0 1984-12-16 12:0:0.0
            cwt.Variable(os.path.join(vcs.sample_data, 'tas_ccsr-95a_1984.01-1984.12.nc'), 'tas'),
        ]

        self.agg_op = cwt.Process(identifier='CDAT.aggregate')

        self.agg_op.set_inputs(*agg_files)

        # (484, 45, 72) 1980-1-1 0:0:0.0 1980-4-30 18:0:0.0
        sub_file = cwt.Variable(os.path.join(vcs.sample_data, 'tas_6h.nc'), 'tas')

        self.sub_op = cwt.Process(identifier='CDAT.subset')

        self.sub_op.set_inputs(sub_file)

        settings.CACHE_PATH = os.path.abspath('./cache')

        if os.path.exists(settings.CACHE_PATH):
            shutil.rmtree(settings.CACHE_PATH)

        os.makedirs(settings.CACHE_PATH)

    def test_aggregate(self):
        proc = process.Process(0)
        
        proc.job = mock.MagicMock()

        with cdms2.open('agg_output.nc', 'w') as out_file, Benchmark() as bm:
            output = proc.retrieve(self.agg_op, len(self.agg_op.inputs), out_file)

            self.assertEqual(out_file[output].shape[0], 72)

        bm.stats()

    #def test_subset(self):
    #    proc = process.Process(0)
    #    
    #    proc.job = mock.MagicMock()

    #    self.agg_op.domain = cwt.Domain([
    #        cwt.Dimension('time', 24, 65, cwt.INDICES),
    #    ])

    #    with cdms2.open('sub_output.nc', 'w') as out_file, Benchmark() as bm:
    #        output = proc.retrieve(self.agg_op, len(self.agg_op.inputs), out_file)

    #        self.assertEqual(out_file[output].shape[0], 41)

    #    bm.stats()
