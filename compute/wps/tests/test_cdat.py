#! /usr/bin/env python

import os
import shutil

import cdms2
import cwt
import mock
import numpy as np
import requests
from contextlib import closing
from contextlib import nested
from django import test

from wps import settings
from wps.processes import CWTBaseTask
from wps.processes import cdat
from wps.tests import cdms2_utils

class TestCDAT(test.TestCase):

    @classmethod
    def setUpClass(cls):
        time1 = cdms2_utils.generate_time(0, 365, 'days since 1990-1-1')

        time2 = cdms2_utils.generate_time(0, 365, 'days since 1991-1-1')

        time3 = cdms2_utils.generate_time(0, 365, 'days since 1992-1-1')

        lat_1 = cdms2_utils.generate_latitude(180, -89.5)

        lon_1 = cdms2_utils.generate_longitude(360, 0)

        cls.d = {
            'd0': {
                'id': 'd0',
                'time': {'start': 100, 'end': 200, 'crs': 'indices'},
                'lat': {'start': 45, 'end': 90, 'crs': 'indices'},
                'lon': {'start': 180, 'end': 270, 'crs': 'indices'},
            },
            'd1': {
                'id': 'd1',
                'time': {'start': 100, 'end': 200, 'crs': 'values'},
                'lat': {'start': 45, 'end': 90, 'crs': 'values'},
                'lon': {'start': 180, 'end': 270, 'crs': 'values'},
            },
            'd2': {
                'id': 'd2',
                'time': {'start': 100, 'end': 900, 'crs': 'indices'},
            },
            'd3': {
                'id': 'd3',
                'time': {'start': 100, 'end': 900, 'crs': 'values'},
            },
        }

        cls.v = {}

        cls.v.update([cdms2_utils.generate_variable(10, (time1, lat_1, lon_1), 'tas', 'tas_1')])
        cls.v.update([cdms2_utils.generate_variable(10, (time2, lat_1, lon_1), 'tas', 'tas_2')])
        cls.v.update([cdms2_utils.generate_variable(10, (time3, lat_1, lon_1), 'tas', 'tas_3')])

        cls.cache_path = os.path.join(os.path.dirname(__file__), 'cache')

        settings.CACHE_PATH = cls.cache_path

    @classmethod
    def tearDownClass(cls):
        for v in cls.v.values():
            os.remove(v['uri'])

    def setUp(self):
        os.mkdir(self.cache_path)

    def tearDown(self):
        shutil.rmtree(self.cache_path)

    @mock.patch('wps.processes.process.CWTBaseTask.set_user_creds')
    def test_aggregate_cache(self, mock):
        o = {
            'CDAT.aggregate': {
                'name': 'CDAT.aggregate',
                'input': ['tas_1', 'tas_2', 'tas_3'],
                'domain': 'd2',
            }
        }

        task = CWTBaseTask()

        file_paths = [self.v['tas_{}'.format(x)]['uri'] for x in range(1, 4)]

        dom = cwt.Domain.from_dict(self.d['d2'])

        cache_files = {}

        with nested(*[closing(cdms2.open(x)) for x in file_paths]) as f:
            domain_map = task.map_domain_multiple(f, 'tas', dom)

        cache_map = {}

        for inp, domain in domain_map.iteritems():
            temporal, spatial = domain

            cache_file, exists = task.check_cache(inp, temporal, spatial)

            self.assertFalse(os.path.exists(cache_file))

            tstart, tstop = temporal.start, temporal.stop

            cache_map[cache_file] = (tstop - tstart, 180, 360)

        result = cdat.aggregate(self.v, o, self.d, local=True)

        for cache_file, domain in cache_map.iteritems():
            self.assertTrue(os.path.exists(cache_file))

            with closing(cdms2.open(cache_file)) as f:
                self.assertEqual(f['tas'].shape, domain)

    @mock.patch('wps.processes.process.CWTBaseTask.set_user_creds')
    def test_aggregate_regrid(self, mock):
        o = {
            'CDAT.aggregate': {
                'name': 'CDAT.aggregate',
                'input': ['tas_1', 'tas_2', 'tas_3'],
                'gridder': {'tool': 'esmf', 'method': 'linear', 'grid': 'gaussian~32'},
            }
        }

        result = cdat.aggregate(self.v, o, self.d, local=True)

        with closing(cdms2.open(result['uri'])) as f:
            self.assertEqual(f['tas'].shape, (1095, 32, 64))

    @mock.patch('wps.processes.process.CWTBaseTask.set_user_creds')
    def test_aggregate_values(self, mock):
        o = {
            'CDAT.aggregate': {
                'name': 'CDAT.aggregate',
                'input': ['tas_1', 'tas_2', 'tas_3'],
                'domain': 'd3',
            }
        }

        result = cdat.aggregate(self.v, o, self.d, local=True)

        with closing(cdms2.open(result['uri'])) as f:
            self.assertEqual(f['tas'].shape, (795, 180, 360))

    @mock.patch('wps.processes.process.CWTBaseTask.set_user_creds')
    def test_aggregate_indices(self, mock):
        o = {
            'CDAT.aggregate': {
                'name': 'CDAT.aggregate',
                'input': ['tas_1', 'tas_2', 'tas_3'],
                'domain': 'd2',
            }
        }

        result = cdat.aggregate(self.v, o, self.d, local=True)

        with closing(cdms2.open(result['uri'])) as f:
            self.assertEqual(f['tas'].shape, (800, 180, 360))

    @mock.patch('wps.processes.process.CWTBaseTask.set_user_creds')
    def test_subset_cache(self, mock):
        o = {
            'CDAT.subset': {
                'name': 'CDAT.subset',
                'input': ['tas_1'],
                'domain': 'd0',
            }
        }

        task = CWTBaseTask()

        file_path = self.v['tas_1']['uri']

        dom = cwt.Domain.from_dict(self.d['d0'])

        with closing(cdms2.open(file_path)) as f:
            temporal, spatial = task.map_domain(f, 'tas', dom)

        cache_file, exists = task.check_cache(file_path, temporal, spatial)

        self.assertFalse(os.path.exists(cache_file)) 

        result = cdat.subset(self.v, o, self.d, local=True)

        self.assertTrue(os.path.exists(cache_file))

        with closing(cdms2.open(cache_file)) as f:
            self.assertEqual(f['tas'].shape, (100, 45, 90))

    @mock.patch('wps.processes.process.CWTBaseTask.set_user_creds')
    def test_subset_regrid(self, mock):
        o = {
            'CDAT.subset': {
                'name': 'CDAT.subset',
                'input': ['tas_1'],
                'gridder': {'tool': 'esmf', 'method': 'linear', 'grid': 'gaussian~32'},
            }
        }

        result = cdat.subset(self.v, o, self.d, local=True)

        with closing(cdms2.open(result['uri'])) as f:
            self.assertEqual(f['tas'].shape, (365, 32, 64))
    
    @mock.patch('wps.processes.process.CWTBaseTask.set_user_creds')
    def test_subset_values(self, mock):
        o = {
            'CDAT.subset': {
                'name': 'CDAT.subset',
                'input': ['tas_1'],
                'domain': 'd1',
            }
        }

        result = cdat.subset(self.v, o, self.d, local=True)

        with closing(cdms2.open(result['uri'])) as f:
            self.assertEqual(f['tas'].shape, (100, 45, 90))

    @mock.patch('wps.processes.process.CWTBaseTask.set_user_creds')
    def test_subset_indices(self, mock):
        o = {
            'CDAT.subset': {
                'name': 'CDAT.subset',
                'input': ['tas_1'],
                'domain': 'd0',
            }
        }

        result = cdat.subset(self.v, o, self.d, local=True)

        with closing(cdms2.open(result['uri'])) as f:
            self.assertEqual(f['tas'].shape, (100, 45, 90))
