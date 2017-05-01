#! /usr/bin/env python

import os

import cdms2
import numpy as np
import requests
from contextlib import closing
from django import test

from wps.processes import cdat

class TestCDAT(test.TestCase):

    def gen_time(self, start, stop, units):
        time = cdms2.createAxis([x for x in xrange(start, stop)])
        time.id = 'time'
        time.units = units
        time.designateTime()

        return time

    def gen_variable(self, value, time, lat, lon, var_name, identifier):
        name = '{}_{}_{}_{}_{}.nc'.format(var_name, value, len(time), len(lat), len(lon))

        path = os.path.join(os.getcwd(), name)

        with closing(cdms2.open(path, 'w')) as f:
            data = np.array([[[value
                               for _ in xrange(len(lat))]
                              for _ in xrange(len(lon))]
                             for _ in xrange(len(time))])

            f.write(data, axes=(time, lon, lat), id=var_name)

        return [(identifier, {'uri': path, 'id': '{}|{}'.format(var_name, identifier)})]

    def setUp(self):
        lat = cdms2.createUniformLatitudeAxis(-89.5, 180, 1.0)

        lon = cdms2.createUniformLongitudeAxis(0, 360, 1.0)

        time = self.gen_time(1, 366, 'days since 2000-1-1')

        self.v = {}
        
        self.v.update(self.gen_variable(10, time, lat, lon, 'tas', 'tas_365_10'))
        self.v.update(self.gen_variable(20, time, lat, lon, 'tas', 'tas_365_20'))

    def test_subset(self):
        o = {'CDAT.subset': {'name': 'CDAT.subset', 'domain': 'd0', 'input': ['tas_365_10']}}

        d = {'d0': {
                    'id': 'd0', 
                    'time': {'start': 200, 'end': 300, 'crs': 'indices'},
                    'latitude': {'start': 0, 'end': 90, 'crs': 'values'},
                    'longitude': {'start': 45, 'end': 224, 'crs': 'values'},
                   }}
        
        result = cdat.subset(self.v, o, d, local=True)

        with closing(cdms2.open(result['uri'])) as f:
            tas = f['tas']

            self.assertEqual(tas.shape, (100, 180, 90))

    def test_avg_domain_spatial_values(self):
        o = {'CDAT.avg': {'name': 'CDAT.avg', 'domain': 'd0', 'input': ['tas_365_10', 'tas_365_20']}}

        d = {'d0': {
                    'id': 'd0', 
                    'latitude': {'start': 0, 'end': 90, 'crs': 'values'},
                    'longitude': {'start': 45, 'end': 224, 'crs': 'values'},
                   }}

        result = cdat.avg(self.v, o, d, local=True)
   
        with closing(cdms2.open(result['uri'])) as f:
            tas = f['tas']

            self.assertEqual(tas.shape, (365, 180, 90))

    def test_avg_domain_spatial_indices(self):
        o = {'CDAT.avg': {'name': 'CDAT.avg', 'domain': 'd0', 'input': ['tas_365_10', 'tas_365_20']}}

        d = {'d0': {
                    'id': 'd0', 
                    'latitude': {'start': 90, 'end': 135, 'crs': 'indices'},
                    'longitude': {'start': 45, 'end': 225, 'crs': 'indices'},
                   }}

        result = cdat.avg(self.v, o, d, local=True)
   
        with closing(cdms2.open(result['uri'])) as f:
            tas = f['tas']

            self.assertEqual(tas.shape, (365, 180, 45))

    def test_avg_domain_time_values(self):
        o = {'CDAT.avg': {'name': 'CDAT.avg', 'domain': 'd0', 'input': ['tas_365_10', 'tas_365_20']}}

        d = {'d0': {'id': 'd0', 'time': {'start': 10, 'end': 110, 'crs': 'values' }}}

        result = cdat.avg(self.v, o, d, local=True)
   
        with closing(cdms2.open(result['uri'])) as f:
            tas = f['tas']

            self.assertEqual(tas.shape, (100, 360, 180))
            self.assertEqual(tas.getTime()[0], 10)

    def test_avg_domain_time_indices_step(self):
        o = {'CDAT.avg': {'name': 'CDAT.avg', 'domain': 'd0', 'input': ['tas_365_10', 'tas_365_20']}}

        d = {'d0': {'id': 'd0', 'time': {'start': 10, 'end': 110, 'crs': 'indices', 'step': 2 }}}

        result = cdat.avg(self.v, o, d, local=True)
   
        with closing(cdms2.open(result['uri'])) as f:
            tas = f['tas']

            self.assertEqual(tas.shape, (50, 360, 180))
            self.assertEqual(tas.getTime()[0], 11)

    def test_avg_domain_time_indices(self):
        o = {'CDAT.avg': {'name': 'CDAT.avg', 'domain': 'd0', 'input': ['tas_365_10', 'tas_365_20']}}

        d = {'d0': {'id': 'd0', 'time': {'start': 10, 'end': 110, 'crs': 'indices' }}}

        result = cdat.avg(self.v, o, d, local=True)
   
        with closing(cdms2.open(result['uri'])) as f:
            tas = f['tas']

            self.assertEqual(tas.shape, (100, 360, 180))
            self.assertEqual(tas.getTime()[0], 11)

    def test_avg(self):
        o = {'CDAT.avg': {'name': 'CDAT.avg', 'input': ['tas_365_10', 'tas_365_20']}}

        d = {}

        result = cdat.avg(self.v, o, d, local=True)
   
        with closing(cdms2.open(result['uri'])) as f:
            tas = f['tas']

            self.assertEqual(tas.shape, (365, 360, 180))
            self.assertEqual(tas[0][0][0], 15)
