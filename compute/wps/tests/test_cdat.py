#! /usr/bin/env python

import os

import cdms2
import numpy as np
import requests
from contextlib import closing
from django import test

from wps.processes import cdat

class TestCDAT(test.TestCase):

    def setUp(self):
        lon = cdms2.createAxis(np.array([x for x in xrange(0, 180)]))

        lat = cdms2.createAxis(np.array([x for x in xrange(-90, 90)]))

        time1 = cdms2.createAxis(np.array([x for x in xrange(0, 365)]))
        time1.units = 'days since 2016-1-1'

        time2 = cdms2.createAxis(np.array([x for x in xrange(0, 730)]))
        time2.units = 'days since 2015-1-1'

        self.tas_365_10 = os.path.join(os.getcwd(), 'tas_365_10.nc')

        with closing(cdms2.open(self.tas_365_10, 'w')) as f:
            tas_365_10 = cdms2.createVariable(np.array([[[10 for _ in xrange(180)] for _ in xrange(180)] for _ in xrange(365)]), axes=(time1, lat, lon), id='tas')

            f.write(tas_365_10)

        self.tas_365_20 = os.path.join(os.getcwd(), 'tas_365_20.nc')

        with closing(cdms2.open(self.tas_365_20, 'w')) as f:
            tas_365_20 = cdms2.createVariable(np.array([[[20 for _ in xrange(180)] for _ in xrange(180)] for _ in xrange(365)]), axes=(time1, lat, lon), id='tas')

            f.write(tas_365_20)

        self.tas_730_20 = os.path.join(os.getcwd(), 'tas_730_20.nc')

        with closing(cdms2.open(self.tas_730_20, 'w')) as f:
            tas_730_20 = cdms2.createVariable(np.array([[[20 for _ in xrange(180)] for _ in xrange(180)] for _ in xrange(730)]), axes=(time2, lat, lon), id='tas')

            f.write(tas_730_20)

        self.v = {
                  'tas1': {'uri': self.tas_365_10, 'id': 'tas|tas1'},
                  'tas2': {'uri': self.tas_365_20, 'id': 'tas|tas2'},
                  'tas3': {'uri': self.tas_730_20, 'id': 'tas|tas3'},
                 }

    def test_avg(self):
        o = {'avg': {'name': 'CDAT.avg', 'input': ['tas1', 'tas2'] }}

        d = {}

        result = cdat.avg(self.v, o, d, local=True)
        
        f = cdms2.open(result['uri'])

        v = f['tas']

        self.assertEqual(v.shape, (365, 180, 180))
        self.assertEqual(v[0][0][0], 15)
