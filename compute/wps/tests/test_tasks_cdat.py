#! /usr/bin/env python

import datetime

import cwt
import mock
from django import test

from wps import tasks

class CDATTaskTestCase(test.TestCase):

    def setUp(self):
        self.attrs1 = {
            'file:///test1.nc': {
                'file:///test1_1.nc': {
                    'elapsed': datetime.timedelta(seconds=3),
                    'size': 1.2,
                },
                'file:///test1_2.nc': {
                    'elapsed': datetime.timedelta(seconds=6),
                    'size': 2.2,
                },
                'file:///test1_3.nc': {
                    'elapsed': datetime.timedelta(seconds=2),
                    'size': 0.2,
                },
            }
        }

        self.operation1 = cwt.Process(identifier='CDAT.regrid')

        self.operation2 = cwt.Process(identifier='CDAT.regrid')
        self.operation2.parameters['gridder'] = cwt.Gridder(grid='gaussian~32')

        self.base_units1 = 'days since 1990-1-1 0'
        
    @mock.patch('cdms2.open')
    @mock.patch('wps.models.Job.objects.get')
    def test_regrid_missing_gridder(self, mock_job, mock_open):
        with self.assertRaises(cwt.ProcessError):
            tasks.regrid(self.attrs1, self.operation1, 'tas', self.base_units1, 0)

    @mock.patch('cdms2.open')
    @mock.patch('wps.models.Job.objects.get')
    def test_regrid(self, mock_job, mock_open):
        result = tasks.regrid(self.attrs1, self.operation2, 'tas', self.base_units1, 0)

        self.assertEqual(mock_open.call_count, 4)

        mock_job.assert_called_with(pk=0)

        mock_job.return_value.succeeded.assert_called()
