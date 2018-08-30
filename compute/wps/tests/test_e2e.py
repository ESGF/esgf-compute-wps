import os
import signal
import time
import unittest

import cdms2
import cwt
from django import test

from wps.tasks import base

class Timeout(object):
    def __init__(self, seconds):
        self.seconds = seconds

    def handle_timeout(self, signum, frame):
        raise Exception('Timed out after %r seconds', self.seconds)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)
    
    def __exit__(self, exc_type, exc_value, traceback):
        signal.alarm(0)

@unittest.skip('Needs to be run explicitly')
class E2EUnitTest(test.TestCase):
    def setUp(self):
        try:
            self.api = os.environ['CWT_API_KEY']
        except KeyError:
            raise Exception('Missing required CWT api key')

        try:
            host = os.environ['WPS_HOST']
        except KeyError:
            raise Exception('Missing host of WPS server')

        self.host = 'https://{}/wps/'.format(host)

        self.variables = [
            cwt.Variable('http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GMAO/output/NASA-GMAO/GEOS-5/decadal1960/mon/atmos/cct/r1i1p1/cct_Amon_GEOS-5_decadal1960_r1i1p1_196101-197012.nc',
                         'cct')
        ]

        self.expected_ops = [
            'CDAT.aggregate',
            'CDAT.subset',
            'CDAT.regrid',
            'CDAT.metrics',
            'CDAT.min',
            'CDAT.max',
            'CDAT.average',
            'CDAT.sum',
        ]

        self.client = cwt.WPSClient(self.host, verify=False, api_key=self.api)

    def test_execute(self):
        op = self.client.processes('CDAT.aggregate')[0]

        domain = cwt.Domain(lat=(0, 90), lon=(90, 270))

        with Timeout(128):
            self.client.execute(op, inputs=self.variables, domain=domain)

            while op.processing:
                time.sleep(2)

            self.assertTrue(op.succeeded)

        with cdms2.open(op.output.uri) as infile:
            print infile[op.output.var_name].shape

    def test_describe_process(self):
        op = self.client.processes('CDAT.aggregate')

        describe = self.client.describe_process(op)

        self.assertEqual(describe[0].identifier, 'CDAT.aggregate')

    def test_get_capabilities(self):
        cap = self.client.get_capabilities()

        for p in cap.processes:
            if p.identifier not in self.expected_ops:
                raise Exception('Missing operation %r', p.identifier)
