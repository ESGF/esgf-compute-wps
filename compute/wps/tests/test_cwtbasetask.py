#! /usr/bin/env python

import os
import shutil
from contextlib import nested

import cdms2
import cwt
from django import test

from wps import models
from wps import processes
from wps import settings
from wps.tests import cdms2_utils as utils

class TestCWTBaseTask(test.TestCase):

    def setUp(self):
        self.server = models.Server()
        self.server.save()

        self.job = models.Job(server=self.server)
        self.job.save()

        self.base = processes.CWTBaseTask()

        self.time1 = utils.generate_time(0, 365, 'days since 1970-1-1')
        self.time2 = utils.generate_time(0, 365, 'days since 1980-1-1')
        self.time3 = utils.generate_time(0, 365, 'days since 1990-1-1')
        self.time4 = utils.generate_time(0, 43800, 'days since 1970-1-1', 365)

        self.lat = utils.generate_latitude(180)

        self.lon = utils.generate_longitude(360)

        self.v = {}
        self.v.update([utils.generate_variable(10, (self.time1, self.lat, self.lon), 'tas', 'tas1')])
        self.v.update([utils.generate_variable(10, (self.time2, self.lat, self.lon), 'tas', 'tas2')])
        self.v.update([utils.generate_variable(10, (self.time3, self.lat, self.lon), 'tas', 'tas3')])

        self.subset_op = cwt.Process.from_dict({
            'result': 'subset',
            'input': ['tas1'],
            'name': 'CDAT.subset',
        })

    def tearDown(self):
        for value in self.v.values():
            os.remove(value['uri'])

    def test_generate_grid_missing(self):
        grid, tool, method = self.base.generate_grid(self.subset_op, {}, {})

        self.assertIsNone(grid)
        self.assertIsNone(tool)
        self.assertIsNone(method)

    def test_generate_grid(self):
        self.subset_op.parameters['gridder'] = cwt.Gridder(grid='gaussian~128')

        grid, tool, method = self.base.generate_grid(self.subset_op, {}, {})

        self.assertIsInstance(grid, cdms2.grid.TransientRectGrid)
        self.assertEqual(tool, 'esmf')
        self.assertEqual(method, 'linear')

    def test_generate_grid_domain(self):
        self.subset_op.parameters['gridder'] = cwt.Gridder(grid='d0')

        domains = {
            'd0': cwt.Domain([
                cwt.Dimension('lat', 0, 180),
                cwt.Dimension('lon', 0, 360),
            ])
        }

        grid, tool, method = self.base.generate_grid(self.subset_op, {}, domains)

        self.assertIsInstance(grid, cdms2.grid.TransientRectGrid)
        self.assertEqual(tool, 'esmf')
        self.assertEqual(method, 'linear')

    def test_generate_grid_variable(self):
        self.subset_op.parameters['gridder'] = cwt.Gridder(grid='tas1')

        variables = {
            'tas1': cwt.Variable(self.v['tas1']['uri'], 'tas'),
        }

        grid, tool, method = self.base.generate_grid(self.subset_op, variables, {})

        self.assertIsInstance(grid, cdms2.grid.FileRectGrid)
        self.assertEqual(tool, 'esmf')
        self.assertEqual(method, 'linear')

    def test_map_domain_unknown_dimension(self):
        input_file = self.v['tas1']['uri']

        domain = cwt.Domain([
            cwt.Dimension('height', 0, 2, cwt.INDICES),
        ])

        with cdms2.open(input_file) as variable, self.assertRaises(Exception):
            temporal, spatial = self.base.map_domain(variable, 'tas', domain)

    def test_map_domain(self):
        input_file = self.v['tas1']['uri']

        domain = cwt.Domain([
            cwt.Dimension('time', 200, 300, cwt.INDICES),
            cwt.Dimension('lat', 0, 90, cwt.INDICES),
            cwt.Dimension('lon', 0, 180, cwt.INDICES)
        ])

        with cdms2.open(input_file) as variable:
            temporal, spatial = self.base.map_domain(variable, 'tas', domain)

            self.assertEqual(temporal, slice(200, 300, 1))
            self.assertDictEqual(spatial, {'lat': slice(0, 90, 1), 'lon': slice(0, 180, 1)})

    def test_map_domain_whole(self):
        input_file = self.v['tas1']['uri']

        with cdms2.open(input_file) as variable:
            temporal, spatial = self.base.map_domain(variable, 'tas', None)

            self.assertEqual(temporal, slice(0, 365, 1))
            self.assertEqual(spatial, {})

    def test_map_domain_multiple_domain(self):
        domain = cwt.Domain([
            cwt.Dimension('time', 200, 800, cwt.INDICES),
            cwt.Dimension('lat', 0, 90, cwt.INDICES),
            cwt.Dimension('lon', 0, 180, cwt.INDICES),
        ])

        with nested(*[cdms2.open(x['uri']) for x in self.v.values()]) as variables:
            domain_map = self.base.map_domain_multiple(variables, 'tas', domain)

            expected_keys = [x['uri'] for x in self.v.values()]

            self.assertItemsEqual(domain_map.keys(), expected_keys)

            self.assertEqual(len(domain_map), 3)

            expected_keys = [x['uri'] for x in self.v.values()]

            self.assertItemsEqual(domain_map.keys(), expected_keys)

            time_slices = [x[0] for x in domain_map.values()]

            self.assertEqual(time_slices, [slice(0, 365, 1), slice(200, 365, 1), slice(0, 70, 1)])

            spatial_slices = [x[1] for x in domain_map.values()]

            truth = reduce(lambda x, y: x if x == y else None, spatial_slices)

            self.assertEqual(len(truth), 2)
            self.assertItemsEqual(truth.keys(), ['lat', 'lon'])
            self.assertEqual(truth['lat'], slice(0, 90, 1))
            self.assertEqual(truth['lon'], slice(0, 180, 1))
        
    def test_map_domain_multiple(self):
        with nested(*[cdms2.open(x['uri']) for x in self.v.values()]) as variables:
            domain_map = self.base.map_domain_multiple(variables, 'tas', None)

            expected_keys = [x['uri'] for x in self.v.values()]

            self.assertItemsEqual(domain_map.keys(), expected_keys)

    def test_map_axis_values(self):
        time = cwt.Dimension('time', 120, 10000)

        time_slice = self.base.map_axis(self.time4, time)

        self.assertEqual(time_slice, slice(1, 28, 1))

    def test_map_axis_indices_out_of_bounds(self):
        time = cwt.Dimension('time', 50, 150, cwt.INDICES)

        with self.assertRaises(Exception):
            time_slice = self.base.map_axis(self.time4, time)

    def test_map_axis_indices(self):
        time = cwt.Dimension('time', 50, 100, cwt.INDICES)

        time_slice = self.base.map_axis(self.time4, time)

        self.assertEqual(time_slice, slice(50, 100, 1))

    def test_check_cache(self):
        cache_path = os.path.join(os.path.dirname(__file__), 'cache')

        if os.path.exists(cache_path):
            shutil.rmtree(cache_path)

        os.mkdir(cache_path)

        settings.CACHE_PATH = cache_path

        uri = '/data/test.nc'

        var_name = 'tas'

        temporal = slice(0, 365, 1)

        spatial = {}

        cache, exists = self.base.check_cache(uri, var_name, temporal, spatial)

        with cache as cache:
            self.assertIsInstance(cache, cdms2.dataset.CdmsFile)
            self.assertFalse(exists)

    def test_generate_cache_name(self):
        uri = '/data/test.nc'

        temporal = slice(100, 300, 2)

        spatial = {
            'lat': slice(120, 200, 3),
            'lon': slice(120, 300, 1),
        }

        file_name = self.base.generate_cache_name(uri, temporal, spatial)

        expected = '00bb92e7a90a5ca3a32f5db165ade08e399cbb2e746eebea80ab1dc6584a5ebd'

        self.assertEqual(file_name, expected)

    def test_cache_input_domain(self):
        cache_path = os.path.join(os.path.dirname(__file__), 'cache')

        if os.path.exists(cache_path):
            shutil.rmtree(cache_path)

        os.mkdir(cache_path)

        settings.CACHE_PATH = cache_path

        variable = cwt.Variable(self.v['tas1']['uri'], 'tas')

        domain = cwt.Domain([
            cwt.Dimension('time', 200, 300, cwt.INDICES),
        ])

        self.base.cache_input(variable, domain)

        cached_files = [os.path.join(cache_path, x) for x in os.listdir(cache_path)]

        self.assertEqual(len(cached_files), 1)

        shutil.rmtree(cache_path)

    def test_cache_input(self):
        cache_path = os.path.join(os.path.dirname(__file__), 'cache')

        if os.path.exists(cache_path):
            shutil.rmtree(cache_path)

        os.mkdir(cache_path)

        settings.CACHE_PATH = cache_path

        variable = cwt.Variable(self.v['tas1']['uri'], 'tas')

        self.base.cache_input(variable, None)

        cached_files = [os.path.join(cache_path, x) for x in os.listdir(cache_path)]

        self.assertEqual(len(cached_files), 1)

        shutil.rmtree(cache_path)

    def test_cache_multiple_input_domain_partial(self):
        cache_path = os.path.join(os.path.dirname(__file__), 'cache')

        if os.path.exists(cache_path):
            shutil.rmtree(cache_path)

        os.mkdir(cache_path)

        settings.CACHE_PATH = cache_path

        variables = [cwt.Variable(x['uri'], 'tas') for x in self.v.values()]

        domain = cwt.Domain([
            cwt.Dimension('time', 200, 500, cwt.INDICES),
        ])

        self.base.cache_multiple_input(variables, domain)

        cached_files = [os.path.join(cache_path, x) for x in os.listdir(cache_path)]

        self.assertEqual(len(cached_files), 2)

        shutil.rmtree(cache_path)

    def test_cache_multiple_input_domain(self):
        cache_path = os.path.join(os.path.dirname(__file__), 'cache')

        if os.path.exists(cache_path):
            shutil.rmtree(cache_path)

        os.mkdir(cache_path)

        settings.CACHE_PATH = cache_path

        variables = [cwt.Variable(x['uri'], 'tas') for x in self.v.values()]

        domain = cwt.Domain([
            cwt.Dimension('time', 200, 830, cwt.INDICES),
        ])

        self.base.cache_multiple_input(variables, domain)

        cached_files = [os.path.join(cache_path, x) for x in os.listdir(cache_path)]

        self.assertEqual(len(cached_files), 3)

        shutil.rmtree(cache_path)

    def test_cache_multiple_input(self):
        cache_path = os.path.join(os.path.dirname(__file__), 'cache')

        if os.path.exists(cache_path):
            shutil.rmtree(cache_path)

        os.mkdir(cache_path)

        settings.CACHE_PATH = cache_path

        variables = [cwt.Variable(x['uri'], 'tas') for x in self.v.values()]

        self.base.cache_multiple_input(variables, None)

        cached_files = [os.path.join(cache_path, x) for x in os.listdir(cache_path)]

        self.assertEqual(len(cached_files), 3)

        shutil.rmtree(cache_path)

    def test_slice_to_str(self):
        s = slice(25, 100, 2)

        output = self.base.slice_to_str(s)

        self.assertEqual(output, '25:100:2')

    def test_generate_output(self):
        local_path = '/data/test.nc'

        path = self.base.generate_output(local_path)

        expected = settings.OUTPUT_URL.format(file_name='test.nc')

        self.assertEqual(path, expected)

    def test_generate_local_output_unique(self):
        path = self.base.generate_local_output()

        self.assertIn(settings.OUTPUT_LOCAL_PATH, path)

    def test_generate_local_output(self):
        path = self.base.generate_local_output('test.nc')

        expected = os.path.join(settings.OUTPUT_LOCAL_PATH, 'test.nc')

        self.assertEqual(path, expected)

    def test_load(self):
        variables = {
            'tas': {
                'uri': 'file:///test.nc',
                'id': 'tas|tas',
            }
        }

        domains = {
            'd0': {
                'id': 'd0',
                'time': {
                    'start': 0,
                    'end': 120,
                    'crs': 'values',
                }
            }
        }

        operations = {
            'subset': {
                'name': 'subset',
                'input': ['tas'],
                'domain': 'd0',
            }
        }

        v, d, o = self.base.load(variables, domains, operations)

        self.assertIn('tas', v)
        self.assertIsInstance(v['tas'], cwt.Variable)
        
        self.assertIn('d0', d)
        self.assertIsInstance(d['d0'], cwt.Domain)

        self.assertIn('subset', o)
        self.assertIsInstance(o['subset'], cwt.Process)

        subset = o['subset']

        self.assertEqual(subset.domain, d['d0'])
        self.assertIsInstance(subset.inputs, list)
        self.assertEqual(len(subset.inputs), 1)
        self.assertEqual(subset.inputs[0], v['tas'])

    def test_set_user_creds(self):
        user = models.User(username='test')
        user.save()

        user.auth = models.Auth(cert='test')
        user.auth.save()

        self.base.set_user_creds(**{
            'cwd': os.path.dirname(__file__),
            'user_id': 1,
        })

        user_path = os.path.join(os.path.dirname(__file__), '1')
        creds_path = os.path.join(user_path, 'creds.pem')
        dodsrc_path = os.path.join(user_path, '.dodsrc')

        self.assertEqual(user_path, os.getcwd())
        self.assertTrue(os.path.exists(user_path))
        self.assertTrue(os.path.exists(creds_path))
        self.assertTrue(os.path.exists(dodsrc_path))

    def test_initialize(self):
        status = self.base.initialize()

        self.assertIsInstance(status, processes.Status)

class TestStatus(test.TestCase):

    def setUp(self):
        self.server = models.Server()
        self.server.save()

        self.job = models.Job(server=self.server)
        self.job.save()

    def test_update(self):
        self.job.status_set.create(status='created')

        status = processes.Status(self.job)

        status.update('test', 50.0)

        self.assertEqual(len(self.job.status_set.all()[0].message_set.all()), 1)

    def test_from_job_id(self):
        status = processes.Status.from_job_id(0)

        self.assertIsNotNone(status)
