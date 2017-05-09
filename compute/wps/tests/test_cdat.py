#! /usr/bin/env python

import os

import cdms2
import numpy as np
import requests
from contextlib import closing
from django import test

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
                           for _ in xrange(len(lat))]
                          for _ in xrange(len(lon))]
                         for _ in xrange(len(time))])

        f.write(data, axes=(time, lon, lat), id=var_name)

    return [(identifier, {'uri':file_path,'id': '{}|{}'.format(var_name, identifier)})]

def generate_grid(value, var_name, identifier):
    file_name = '{}.nc'.format(identifier)

    file_path = '{}/{}'.format(os.path.dirname(__file__), file_name)

    grid, arg = value.split('~')

    if grid == 'gaussian':
        g = cdms2.createGaussianGrid(int(arg))

        g.writeToFile(file_path)

    return [(identifier, {'uri':file_path,'id': '{}|{}'.format(var_name, identifier)})]

class TestCDAT(test.TestCase):

    def setUp(self):
        time1 = generate_time(0, 365, 'days since 1990-1-1')

        lat_1 = cdms2.createUniformLatitudeAxis(-89.5, 180, 1)

        lon_1 = cdms2.createUniformLongitudeAxis(0.5, 360, 1)

        self.v = {}

        self.v.update(generate_variable(10, time1, lat_1, lon_1, 'tas', 'tas_10_365_180_360'))
        self.v.update(generate_grid('gaussian~32', 'tas', 'weird_grid'))

    def test_subset_bad_time_indices(self):
        o = {'CDAT.subset':{'name':'CDAT.subset','domain':'d0','input':['tas_10_365_180_360']}}

        d = {'d0':{'id':'d0',
                   'time':{'start':0,'end':500,'crs':'indices'}
                  }}

        r = cdat.subset(self.v, o, d, local=True)

        with closing(cdms2.open(r['uri'], 'r')) as f:
            tas = f['tas']

            self.assertEqual(tas.shape, (365, 180, 360))

    def test_subset_regrid_file(self):
        o = {'CDAT.subset':{'name':'CDAT.subset',
                            'domain':'d0',
                            'input':['tas_10_365_180_360'],
                            'gridder':{'tool':'esmf','method':'linear','grid':'weird_grid'}}}

        d = {'d0':{'id':'d0',
                   'time':{'start':100,'end':300,'crs':'indices'},
                   'latitude':{'start':0,'end':90,'crs':'indices'},
                   'longitude':{'start':180,'end':270,'crs':'indices'},
                  }}

        r = cdat.subset(self.v, o, d, local=True)

        with closing(cdms2.open(r['uri'], 'r')) as f:
            tas = f['tas']

            self.assertEqual(tas.shape, (200, 64, 32))

    def test_subset_regrid(self):
        o = {'CDAT.subset':{'name':'CDAT.subset',
                            'domain':'d0',
                            'input':['tas_10_365_180_360'],
                            'gridder':{'tool':'esmf','method':'linear','grid':'gaussian~32'}}}

        d = {'d0':{'id':'d0',
                   'time':{'start':100,'end':300,'crs':'indices'},
                   'latitude':{'start':0,'end':90,'crs':'indices'},
                   'longitude':{'start':180,'end':270,'crs':'indices'},
                  }}

        r = cdat.subset(self.v, o, d, local=True)

        with closing(cdms2.open(r['uri'], 'r')) as f:
            tas = f['tas']

            self.assertEqual(tas.shape, (200, 32, 64))

    def test_subset_values(self):
        o = {'CDAT.subset':{'name':'CDAT.subset','domain':'d0','input':['tas_10_365_180_360']}}

        d = {'d0':{'id':'d0',
                   'time':{'start':100,'end':300,'crs':'values'},
                   'latitude':{'start':-90,'end':0,'crs':'values'},
                   'longitude':{'start':180,'end':270,'crs':'values'},
                  }}

        r = cdat.subset(self.v, o, d, local=True)

        with closing(cdms2.open(r['uri'], 'r')) as f:
            tas = f['tas']

            self.assertEqual(tas.shape, (201, 90, 90))

    def test_subset_indices(self):
        o = {'CDAT.subset':{'name':'CDAT.subset','domain':'d0','input':['tas_10_365_180_360']}}

        d = {'d0':{'id':'d0',
                   'time':{'start':100,'end':300,'crs':'indices'},
                   'latitude':{'start':0,'end':90,'crs':'indices'},
                   'longitude':{'start':180,'end':270,'crs':'indices'},
                  }}

        r = cdat.subset(self.v, o, d, local=True)

        with closing(cdms2.open(r['uri'], 'r')) as f:
            tas = f['tas']

            self.assertEqual(tas.shape, (200, 90, 90))
