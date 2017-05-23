#! /usr/bin/env python

import os

import cdms2
import cwt
from django import test

from wps.processes import CWTBaseTask
from wps.tests import cdms2_utils

class CWTBaseTaskTestCase(test.TestCase):

    def cdms2_open(self, var):
        f = cdms2.open(var[1]['uri'])

        return f

    def setUp(self):
        lat = cdms2_utils.generate_latitude(180)

        lon = cdms2_utils.generate_longitude(360)

        time = cdms2_utils.generate_time(0, 365, 'days since 1990-1-1')

        self.var = cdms2_utils.generate_variable(10, (time, lat, lon), 'tas', 'tas_1')

        self.task = CWTBaseTask()

    def tearDown(self):
        os.remove(self.var[1]['uri'])

    def test_check_cache(self):
        t = slice(0, 100, 1)

        s = {
            'lat': slice(0, 45, 1),
            'lon': slice(0, 180, 1),
        }

        cache_file, exists = self.task.check_cache('/test.nc', t, s)

        self.assertEqual(cache_file, '/data/cache/5eb50aed6ebe94f67154d7f3516becbcb01e0bac29747bcb7dc0ee609b02cbd0.nc')
        self.assertFalse(exists)

    def test_map_axis_out_of_bounds_indices(self):
        f = self.cdms2_open(self.var)

        axis = f['tas'].getAxis(0)

        low = cwt.Dimension('time', -1, 365, cwt.INDICES)

        with self.assertRaises(Exception):
            self.task.map_axis(axis, low)

        high = cwt.Dimension('time', 0, 366, cwt.INDICES)

        with self.assertRaises(Exception):
            self.task.map_axis(axis, high)

        swap = cwt.Dimension('time', 365, 0, cwt.INDICES)

        with self.assertRaises(Exception):
            self.task.map_axis(axis, swap)

    def test_map_axis_bad_crs(self):
        f = self.cdms2_open(self.var)

        axis = f['tas'].getAxis(0)

        dim = cwt.Dimension('time', 10, 20, 'monkeys')

        with self.assertRaises(Exception):
            self.task.map_axis(axis, dim)

    def test_map_domain_no_domain(self):
        f = self.cdms2_open(self.var)

        t, s = self.task.map_domain(f, 'tas', None)

        self.assertEqual(t, slice(0, 365, 1))

        self.assertDictEqual(s, {})

    def test_map_domain_non_existant_dim(self):
        dom_bad_dim = cwt.Domain([
            cwt.Dimension('monkey', 10, 20)
        ])

        f = self.cdms2_open(self.var)

        with self.assertRaises(Exception):
            self.task.map_domain(f, 'tas', dom_bad_dim)

    def test_map_domain_values(self):
        vdom = cwt.Domain([
            cwt.Dimension('lat', -45, 45, cwt.VALUES),
            cwt.Dimension('lon', 90, 270, cwt.VALUES),
            cwt.Dimension('time', 100, 200, cwt.VALUES),
        ])

        f = self.cdms2_open(self.var)

        t, s = self.task.map_domain(f, 'tas', vdom)

        self.assertEqual(t, slice(100, 200, 1))

        self.assertDictEqual({
            'lat': slice(45, 135, 1),
            'lon': slice(90, 270, 1),
        }, s)

    def test_map_domain_indices(self):
        self.idom = cwt.Domain([
            cwt.Dimension('lat', 45, 90, cwt.INDICES),
            cwt.Dimension('lon', 90, 270, cwt.INDICES),
            cwt.Dimension('time', 100, 200, cwt.INDICES),
        ])

        f = self.cdms2_open(self.var)

        t, s = self.task.map_domain(f, 'tas', self.idom)

        self.assertEqual(t, slice(100, 200, 1))

        self.assertDictEqual({
            'lat': slice(45, 90, 1),
            'lon': slice(90, 270, 1),
        }, s)
