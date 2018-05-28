#! /usr/bin/env python

import multiprocessing
import os
import shutil
import time

import cdms2
import cwt
import django
import mock
import psutil
import sys
import vcs
from cdms2 import MV2 as MV

vcs.download_sample_data_files()

class Benchmark(object):
    def __init__(self, filename):
        self.fd = None
        self.wrote_headers = False
        self.filename = filename

    def __enter__(self):
        self.fd = open(self.filename, 'w')    

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.fd.flush()

        self.fd.close()

    def write(self, d):
        if not self.wrote_headers:
            self.wrote_headers = True

            self.fd.write(','.join([
                'time',
                'cpu_percent',
                'cpu_user_time',
                'cpu_system_time',
                'io_read_count',
                'io_write_count',
                'io_read_bytes',
                'io_write_bytes',
                'mem_rss',
                'mem_vms',
                'mem_shared',
                'mem_text',
                'mem_lib',
                'mem_data',
                'mem_dirty',
                'mem_uss',
                'mem_pss',
                'mem_swap',
                'mem_percent',
                'num_ctx_switches_voluntary',
                'num_ctx_switches_involuntary',
                'num_fds',
                'num_threds'
            ]) + '\n')

        data = [
            str(time.time()),
            str(d['cpu_percent']),
            str(d['cpu_times'].user),
            str(d['cpu_times'].system)
        ]

        if d['io_counters'] is None:
            data.extend([str(None)]*4)
        else:
            data.extend([
                str(d['io_counters'].read_count),
                str(d['io_counters'].write_count),
                str(d['io_counters'].read_bytes),
                str(d['io_counters'].write_bytes),
            ])

        if d['memory_full_info'] is None:
            data.extend([str(None)]*10)
        else:
            data.extend([
                str(d['memory_full_info'].rss),
                str(d['memory_full_info'].vms),
                str(d['memory_full_info'].shared),
                str(d['memory_full_info'].text),
                str(d['memory_full_info'].lib),
                str(d['memory_full_info'].data),
                str(d['memory_full_info'].dirty),
                str(d['memory_full_info'].uss),
                str(d['memory_full_info'].pss),
                str(d['memory_full_info'].swap),
            ])

        data.extend([
            str(d['memory_percent']),
            str(d['num_ctx_switches'].voluntary),
            str(d['num_ctx_switches'].involuntary),
            str(d['num_fds']),
            str(d['num_threads'])
        ])

        self.fd.write(','.join(data) + '\n')

sys.path.insert(0, os.path.abspath('../../'))

os.environ['DJANGO_SETTINGS_MODULE'] = 'compute.settings'

os.environ['DJANGO_CONFIG_PATH'] = '../../../docker/common/django.properties'

os.environ['WPS_TEST'] = 'test'

django.setup()

from django.conf import settings
from wps.tasks import process

settings.WPS_CACHE_PATH = os.path.abspath('./cache')

output_dir = os.path.abspath('./output')

if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

TEST_OUTPUT = os.path.join(os.path.abspath('./'), 'benchmark_result.nc')

ps_fields = [
    'cpu_percent',
    'cpu_times',
    'io_counters',
    'memory_full_info',
    'memory_percent',
    'num_ctx_switches',
    'num_fds',
    'num_threads'
]

def clean():
    if os.path.exists(settings.WPS_CACHE_PATH):
        shutil.rmtree(settings.WPS_CACHE_PATH)

    os.makedirs(settings.WPS_CACHE_PATH)

    if os.path.exists(TEST_OUTPUT):
        os.remove(TEST_OUTPUT)

def execute(output, test_func):
    manager = multiprocessing.Manager()

    value = manager.Value('s', '')

    p = multiprocessing.Process(target=test_func, args=(value,))

    with Benchmark(output) as bm:
        p.start()

        proc = psutil.Process(pid=p.pid)

        while p.is_alive():
            bm.write(proc.as_dict(ps_fields))

        p.join()

    return value.value

dataset1 = [
    # (12, 1, 32, 64) 1979-1-16 12:0:0.0 1979-12-16 12:0:0.0 days since 1979-1-1 0
    cwt.Variable(os.path.join(vcs.sample_data, 'tas_ccsr-95a_1979.01-1979.12.nc'), 'tas'),
    cwt.Variable(os.path.join(vcs.sample_data, 'tas_ccsr-95a_1980.01-1980.12.nc'), 'tas'),
    cwt.Variable(os.path.join(vcs.sample_data, 'tas_ccsr-95a_1981.01-1981.12.nc'), 'tas'),
    cwt.Variable(os.path.join(vcs.sample_data, 'tas_ccsr-95a_1982.01-1982.12.nc'), 'tas'),
    cwt.Variable(os.path.join(vcs.sample_data, 'tas_ccsr-95a_1983.01-1983.12.nc'), 'tas'),
    # (12, 1, 32, 64) 1984-1-16 12:0:0.0 1984-12-16 12:0:0.0
    cwt.Variable(os.path.join(vcs.sample_data, 'tas_ccsr-95a_1984.01-1984.12.nc'), 'tas'),
]

# (484, 45, 72) 1980-1-1 0:0:0.0 1980-4-30 18:0:0.0
dataset2 = [
    cwt.Variable(os.path.join(vcs.sample_data, 'tas_6h.nc'), 'tas')
]

dataset3 = [
    cwt.Variable(os.path.join(vcs.sample_data, 'tas_dnm-95a_1979.01-1979.12.nc'), 'tas'),
]

dataset4 = [
    cwt.Variable(os.path.join(vcs.sample_data, 'tas_ccsr-95a_1979.01-1979.12.nc'), 'tas'),
]

dataset1_domain1 = cwt.Domain([
    cwt.Dimension('time', 20, 1060),
])

dataset1_domain2 = cwt.Domain([
    cwt.Dimension('time', 20, 1060),
    cwt.Dimension('lat', -90, 0),
    cwt.Dimension('lon', 180, 270),
])

dataset2_domain1 = cwt.Domain([
    cwt.Dimension('time', 20, 400),
])

dataset2_domain2 = cwt.Domain([
    cwt.Dimension('time', 20, 400),
    cwt.Dimension('lat', -90, 0),
    cwt.Dimension('lon', 180, 270),
])

GAUSS_32 = cwt.Gridder(grid='gaussian~32')
GAUSS_16 = cwt.Gridder(grid='gaussian~16')

benchmark_retrieval = [
    ('CDAT.subset', dataset2, None, None, None, (484, 45, 72), 'subset'),
    ('CDAT.subset', dataset2, dataset2_domain1, None, None, (141, 45, 72), 'subset_domain1'),
    ('CDAT.subset', dataset2, dataset2_domain2, None, None, (141, 23, 19), 'subset_domain2'),
    ('CDAT.subset', dataset2, None, None, GAUSS_32, (484, 32, 64), 'subset_regrid'),
    ('CDAT.subset', dataset2, dataset2_domain1, None, GAUSS_32, (141, 32, 64), 'subset_domain1_regrid'),
    ('CDAT.subset', dataset2, dataset2_domain2, None, GAUSS_32, (141, 16, 17), 'subset_domain2_regrid'),
    ('CDAT.aggregate', dataset1, None, None, None, (72, 1, 32, 64), 'aggregate'),
    ('CDAT.aggregate', dataset1, dataset1_domain1, None, None, (34, 1, 32, 64), 'aggregate_domain1'),
    ('CDAT.aggregate', dataset1, dataset1_domain2, None, None, (34, 1, 16, 17), 'aggregate_domain2'),
    ('CDAT.aggregate', dataset1, None, None, GAUSS_16, (72, 1, 16, 32), 'aggregate_regrid'),
    ('CDAT.aggregate', dataset1, dataset1_domain1, None, GAUSS_16, (34, 1, 16, 32), 'aggregate_domain1_regrid'),
    ('CDAT.aggregate', dataset1, dataset1_domain2, None, GAUSS_16, (34, 1, 8, 9), 'aggregate_domain2_regrid'),
]

for identifier, inputs, domain, param, gridder, result, stats in benchmark_retrieval:
    clean()

    def test(value):
        proc = process.Process(0)

        proc.job = mock.MagicMock()

        op = cwt.Process(identifier=identifier)

        op.set_inputs(*inputs)

        if domain is not None:
            op.domain = domain

        if param is not None:
            op.parameters[param.name] = param

        if gridder is not None:
            op.parameters['gridder'] = gridder

        with cdms2.open(TEST_OUTPUT, 'w') as out_file:
            value.value = proc.retrieve(op, len(op.inputs), out_file)

    stats = os.path.join(output_dir, '{}.txt'.format(stats))

    var_name = execute(stats, test)

    with cdms2.open(TEST_OUTPUT) as in_file:
        if in_file[var_name].shape != result:
            raise Exception('File shape {} does not match {} test {}'.format(in_file[var_name].shape, result, stats))

benchmark_process = [
    ('CDAT.max', dataset2, 1, None, cwt.NamedParameter('axes', 'time'), None, MV.max, (45, 72), 'max'),
    ('CDAT.max', dataset2, 1, dataset2_domain1, cwt.NamedParameter('axes', 'time'), None, MV.max, (45, 72), 'max_domain1'),
    ('CDAT.max', dataset2, 1, dataset2_domain2, cwt.NamedParameter('axes', 'time'), None, MV.max, (23, 19), 'max_domain2'),
]

for identifier, inputs, num_inputs, domain, param, gridder, operation, result, stats in benchmark_process:
    clean()

    def test(value):
        proc = process.Process(0)

        proc.job = mock.MagicMock()

        op = cwt.Process(identifier=identifier)

        op.set_inputs(*inputs)

        if domain is not None:
            op.domain = domain

        if param is not None:
            op.parameters[param.name] = param

        if gridder is not None:
            op.parameters['gridder'] = gridder

        with cdms2.open(TEST_OUTPUT, 'w') as out_file:
            value.value = proc.process(op, num_inputs, out_file, operation)

    stats = os.path.join(output_dir, '{}.txt'.format(stats))

    var_name = execute(stats, test)

    with cdms2.open(TEST_OUTPUT) as in_file:
        if in_file[var_name].shape != result:
            raise Exception('File shape {} does not match {} test {}'.format(in_file[var_name].shape, result, stats))
