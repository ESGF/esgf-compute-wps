#! /usr/bin/env python

import os
import shutil

import cdms2
import mock
import numpy as np
import requests
from contextlib import closing
from django import test

from wps import settings
from wps.processes import cdat

def generate_time(start, stop, units):
    time = cdms2.createAxis(np.array([x for x in xrange(start, stop)]))

    time.designateTime()

    time.id = 'time'

    time.units = '{} 0'.format(units)

    return time

def generate_variable(value, time, lat, lon, var_name, identifier):
    file_name = '{}.nc'.format(identifier)

    file_path = '{}/{}'.format(os.path.dirname(__file__), file_name)

    with closing(cdms2.open(file_path, 'w')) as f:
        data = np.array([[[value
                           for _ in xrange(len(lon))]
                          for _ in xrange(len(lat))]
                         for _ in xrange(len(time))])

        f.write(data, axes=(time, lat, lon), id=var_name)

    return [(identifier, {'uri':file_path,'id': '{}|{}'.format(var_name, identifier)})]

class TestCDAT(test.TestCase):

    def setUp(self):
        settings.CACHE_PATH = os.path.join(os.path.dirname(__file__), 'cache')

        self.cache = settings.CACHE_PATH

        os.mkdir(settings.CACHE_PATH)

        time1 = generate_time(0, 365, 'days since 1990-1-1')

        time2 = generate_time(0, 365, 'days since 1991-1-1')

        time3 = generate_time(0, 365, 'days since 1992-1-1')

        lat_1 = cdms2.createUniformLatitudeAxis(-89.5, 180, 1)

        lon_1 = cdms2.createUniformLongitudeAxis(0.5, 360, 1)

        self.d = {'d0': {'id': 'd0',
                         'time': {'start': 100, 'end': 200, 'crs': 'indices'},
                         'lat': {'start': 0, 'end': 90, 'crs': 'indices'},
                         'lon': {'start': 180, 'end': 360, 'crs': 'indices'},
                        },
                  'd1': {'id': 'd1',
                         'time': {'start': 100, 'end': 200, 'crs': 'values'},
                         'latitude': {'start': -90, 'end': 0, 'crs': 'values'},
                         'longitude': {'start': 180, 'end': 360, 'crs': 'values'},
                        },
                  'd2': {'id': 'd2',
                         'latitude': {'start': 90, 'end': 0, 'crs': 'values'},
                        },
                  'd3': {'id': 'd3',
                         'time': {'start': 200, 'end': 850, 'crs': 'indices'},
                        },
                 }

        self.v = {}

        self.v.update(generate_variable(10, time1, lat_1, lon_1, 'tas', 'tas_10_365_180_360'))
        self.v.update(generate_variable(10, time2, lat_1, lon_1, 'tas', 'tas_20_365_180_360'))
        self.v.update(generate_variable(10, time2, lat_1, lon_1, 'tas', 'tas_10_365_180_360_2'))
        self.v.update(generate_variable(10, time3, lat_1, lon_1, 'tas', 'tas_10_365_180_360_3'))

    def tearDown(self):
        for v in self.v.values():
            os.remove(v['uri'])

        shutil.rmtree(self.cache) 

    @mock.patch('wps.processes.process.CWTBaseTask.set_user_creds')
    def test_aggregate(self, mock):
        o = {'CDAT.aggregate': {'name': 'CDAT.aggregate',
                                'input': ['tas_10_365_180_360', 'tas_10_365_180_360_2', 'tas_10_365_180_360_3'],
                                'domain': 'd3',
                               }
            }

        result = cdat.aggregate(self.v, o, self.d, local=True)

        with closing(cdms2.open(result['uri'])) as f:
            self.assertEqual(f['tas'].shape, (650, 180, 360))

    @mock.patch('wps.processes.process.CWTBaseTask.set_user_creds')
    def test_subset_bad_domain_indexes(self, mock):
        o = {'CDAT.subset': {'name': 'CDAT.subset',
                             'input': ['tas_10_365_180_360'],
                             'domain': 'd2',
                            },
            }

        with self.assertRaises(Exception): 
            cdat.subset(self.v, o, self.d, local=True)

    @mock.patch('wps.processes.process.CWTBaseTask.set_user_creds')
    def test_subset_missing_variable(self, mock):
        o = {'CDAT.subset': {'name': 'CDAT.subset',
                             'input': ['tas_10_365_180_360'],
                             'domain': 'd0',
                            },
            }

        self.v['tas_10_365_180_360']['id'] = 'tas1|tas_10_365_180_360'

        with self.assertRaises(Exception):
            cdat.subset(self.v, o, self.d, local=True)
    
    @mock.patch('wps.processes.process.CWTBaseTask.set_user_creds')
    def test_subset_missing_input_file(self, mock):
        o = {'CDAT.subset': {'name': 'CDAT.subset',
                             'input': ['tas'],
                             'domain': 'd0',
                            },
            }

        v = {'tas': {'uri': 'file:///no.nc',
                     'id': 'tas|tas',
                    }
            }

        with self.assertRaises(Exception):
            cdat.subset(v, o, self.d, local=True)

    @mock.patch('wps.processes.process.CWTBaseTask.set_user_creds')
    def test_subset_regrid(self, mock):
        o = {'CDAT.subset': {'name': 'CDAT.subset',
                             'input': ['tas_10_365_180_360'],
                             'domain': 'd1',
                             'gridder': {'tool': 'esmf', 'method': 'linear', 'grid': 'gaussian~32'}
                            },
            }

        result = cdat.subset(self.v, o, self.d, local=True)

        with cdms2.open(result['uri']) as f:
            self.assertEqual(f['tas'].shape, (100, 32, 64))

    @mock.patch('wps.processes.process.CWTBaseTask.set_user_creds')
    def test_subset_values(self, mock):
        o = {'CDAT.subset': {'name': 'CDAT.subset',
                             'input': ['tas_10_365_180_360'],
                             'domain': 'd1'},
            }

        result = cdat.subset(self.v, o, self.d, local=True)

        with cdms2.open(result['uri']) as f:
            self.assertEqual(f['tas'].shape, (100, 90, 180))

    @mock.patch('wps.processes.process.CWTBaseTask.set_user_creds')
    def test_subset_indices(self, mock):
        o = {'CDAT.subset': {'name': 'CDAT.subset',
                             'input': ['tas_10_365_180_360'],
                             'domain': 'd0'},
            }

        result = cdat.subset(self.v, o, self.d, local=True)

        with cdms2.open(result['uri']) as f:
            self.assertEqual(f['tas'].shape, (100, 90, 180))
