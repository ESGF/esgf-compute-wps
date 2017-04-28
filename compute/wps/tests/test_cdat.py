#! /usr/bin/env python

import os

import cdms2
import requests
from django import test

from wps.processes import cdat

class TestCDAT(test.TestCase):

    def setUp(self):
        self.sample_path = os.path.join(os.getcwd(), 'tas_6h.nc')

        response = requests.get('https://uvcdat.llnl.gov/cdat/sample_data/tas_6h.nc')

        with open(self.sample_path, 'w') as f:
            for chunk in response.iter_content(512000):
                f.write(chunk)

    def test_avg(self):
        v = {'tas': {'uri': self.sample_path, 'id': 'tas|tas'}}

        o = {'avg': {'name': 'CDAT.avg', 'input': ['tas'] }}

        d = {}

        result = cdat.avg(v, o, d, local=True)

        f = cdms2.open(result['uri'])

        v = f['tas']

        self.assertEqual(v.shape, (484, 45, 72))
