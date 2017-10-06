import os
import random
import re
import shutil

import cdms2
import cwt
import mock
import numpy as np
from django import test
from django.db.models import Count

from wps import models
from wps import processes
from wps.processes import process
from wps import settings

@process.cwt_shared_task()
def task_cannot_publish(self, **kwargs):
    self.PUBLISH = process.FAILURE | process.RETRY

    self.on_success('', '', (), kwargs)

@process.cwt_shared_task()
def task_success(self, **kwargs):
    self.PUBLISH = process.SUCCESS

    self.on_success('', '', (), kwargs)

@process.cwt_shared_task()
def task_failure(self, **kwargs):
    self.PUBLISH = process.FAILURE

    self.on_failure(Exception('failed'), '', (), kwargs, None)

@process.cwt_shared_task()
def task_retry(self, **kwargs):
    self.PUBLISH = process.RETRY

    self.on_retry(Exception('retry'), '', (), kwargs, None)

class CWTBaseTaskTestCase(test.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.path.exists(settings.CACHE_PATH):
            shutil.rmtree(settings.CACHE_PATH)

        os.makedirs(settings.CACHE_PATH)

        cls.longitude = cdms2.createUniformLongitudeAxis(-180.0, 360.0, 1.0)

        cls.latitude = cdms2.createUniformLatitudeAxis(-90.0, 180.0, 1.0)

        cls.time1 = cdms2.createAxis(np.array([x for x in xrange(24)]))
        cls.time1.id = 'time'
        cls.time1.designateTime()
        cls.time1.units = 'months since 1990-1-1'

        with cdms2.open('./test1.nc', 'w') as outfile:
            outfile.write(
                np.array([[[random.random() for _ in xrange(360)] for _ in xrange(180)] for _ in xrange(24)]),
                axes=(cls.time1, cls.latitude, cls.longitude),
                id='tas'
            )

        cls.time2 = cdms2.createAxis(np.array([x for x in xrange(24)]))
        cls.time2.id = 'time'
        cls.time2.designateTime()
        cls.time2.units = 'months since 1992-1-1'

        with cdms2.open('./test2.nc', 'w') as outfile:
            outfile.write(
                np.array([[[random.random() for _ in xrange(360)] for _ in xrange(180)] for _ in xrange(24)]),
                axes=(cls.time2, cls.latitude, cls.longitude),
                id='tas'
            )

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(settings.CACHE_PATH)

        try:
            os.remove('./test1.nc')
        except:
            pass

        try:
            os.remove('./test2.nc')
        except:
            pass

        try:
            os.remove('./output1.nc')
        except:
            pass

        try:
            os.remove('./multiple.nc')
        except:
            pass

    def setUp(self):
        self.task = processes.CWTBaseTask()

        self.user = models.User.objects.create(username='test')

        models.Auth.objects.create(user=self.user, cert='test')

        server = models.Server.objects.create(host='test', status=0)

        process = models.Process.objects.create(identifier='CDAT.subset', backend='local')

        self.job = models.Job.objects.create(server=server, user=self.user, process=process)

        self.job.accepted()

    def test_task_cannot_publish(self):
        with self.assertNumQueries(0):
            task_cannot_publish(job_id=self.job.pk)

        usage = self.job.process.get_usage(rollover=False)

        self.assertEqual(usage.executed, 1)
        self.assertEqual(usage.retry, 0)
        self.assertEqual(usage.failed, 0)
        self.assertEqual(usage.success, 0)

    def test_task_retry(self):
        self.job.started()

        with self.assertNumQueries(7):
            task_retry(job_id=self.job.pk)

        usage = self.job.process.get_usage(rollover=False)

        self.assertEqual(usage.executed, 1)
        self.assertEqual(usage.retry, 1)

    def test_task_failure(self):
        with self.assertNumQueries(6):
            task_failure(job_id=self.job.pk)

        usage = self.job.process.get_usage(rollover=False)

        self.assertEqual(usage.executed, 1)
        self.assertEqual(usage.failed, 1)

    def test_task_success(self):
        with self.assertNumQueries(6):
            task_success(job_id=self.job.pk)

        usage = self.job.process.get_usage(rollover=False)

        self.assertEqual(usage.executed, 1)
        self.assertEqual(usage.success, 1)

    def test_cache_multiple_input_partial(self):
        variables = [
            cwt.Variable('./test1.nc', 'tas'),
            cwt.Variable('./test2.nc', 'tas')
        ]

        domain = cwt.Domain([
            cwt.Dimension('time', 10, 15),
            cwt.Dimension('lat', -45, 45),
            cwt.Dimension('lon', -90, 90)
        ])

        status = process.Status.from_job_id(self.job.pk)

        status.job.started()

        with cdms2.open('./multiple.nc', 'w') as outfile:
            def callback(data):
                outfile.write(data, id='tas')

            with self.assertNumQueries(14):
                cached = self.task.cache_multiple_input(variables, domain, status, callback)

            for c in cached: self.assertTrue(os.path.exists(c))

    def test_cache_multiple_input(self):
        variables = [
            cwt.Variable('./test1.nc', 'tas'),
            cwt.Variable('./test2.nc', 'tas')
        ]

        domain = cwt.Domain([
            cwt.Dimension('time', 10, 40),
            cwt.Dimension('lat', -45, 45),
            cwt.Dimension('lon', -90, 90)
        ])

        status = process.Status.from_job_id(self.job.pk)

        status.job.started()

        with cdms2.open('./multiple.nc', 'w') as outfile:
            def callback(data):
                outfile.write(data, id='tas')

            with self.assertNumQueries(30):
                cached = self.task.cache_multiple_input(variables, domain, status, callback)

            for c in cached:
                self.assertTrue(os.path.exists(c))

        for c in cached:
            uid = c.split('/')[-1].replace('.nc', '')

            cache = models.Cache.objects.get(uid=uid)

            size = os.stat(cache.local_path).st_size

            self.assertEqual(cache.size, size)

    def test_cache_input_access_error(self):
        variable = cwt.Variable('./test10.nc', 'tas')

        domain = cwt.Domain([
            cwt.Dimension('time', 10, 15),
            cwt.Dimension('lat', -45, 45),
            cwt.Dimension('lon', -90, 90),
        ])

        with cdms2.open('./output1.nc', 'w') as outfile, self.assertRaises(process.AccessError):
            def callback(data):
                outfile.write(data, id='tas')

            self.task.cache_input(variable, domain, callback)

    def test_cache_input_exists(self):
        variable = cwt.Variable('./test1.nc', 'tas')

        domain = cwt.Domain([
            cwt.Dimension('time', 10, 15),
            cwt.Dimension('lat', -45, 45),
            cwt.Dimension('lon', -90, 90),
        ])

        status = process.Status.from_job_id(self.job.pk)

        status.job.started()

        with cdms2.open('./output1.nc', 'w') as outfile:
            def callback(data):
                outfile.write(data, id='tas')

            self.task.cache_input(variable, domain, status, callback)

            with self.assertNumQueries(13):
                cache = self.task.cache_input(variable, domain, status, callback)

    def test_cache_input(self):
        variable = cwt.Variable('./test1.nc', 'tas')

        domain = cwt.Domain([
            cwt.Dimension('time', 10, 15),
            cwt.Dimension('lat', -45, 45),
            cwt.Dimension('lon', -90, 90),
        ])

        status = process.Status.from_job_id(self.job.pk)

        status.job.started()

        with cdms2.open('./output1.nc', 'w') as outfile:
            def callback(data):
                outfile.write(data, id='tas')

            with self.assertNumQueries(18):
                cache = self.task.cache_input(variable, domain, status, callback)

            self.assertEqual(outfile['tas'].shape, (6, 91, 181))

        uid = cache.split('/')[-1].replace('.nc', '')

        cache = models.Cache.objects.get(uid=uid)

        size = os.stat(cache.local_path).st_size

        self.assertEqual(cache.size, size)

    def test_check_cache_fail_time_validation(self):
        uri = './test1.nc'
        var_name = 'tas'
        temporal = slice(0, 16, 1)
        spatial = { 'latitude': (-90, 90), 'longitude': (-180, 180) }

        with self.assertNumQueries(5):
            cached, exists = self.task.check_cache(uri, var_name, temporal, spatial)        

        shutil.copyfile('./test1.nc', cached.local_path)

        with self.assertNumQueries(1):
            cached, exists = self.task.check_cache(uri, var_name, temporal, spatial)        

        self.assertFalse(exists)

    def test_check_cache_exists(self):
        uri = './test1.nc'
        var_name = 'tas'
        temporal = slice(0, 24, 1)
        spatial = { 'latitude': (-90, 90), 'longitude': (-180, 180) }

        with self.assertNumQueries(5):
            cached, exists = self.task.check_cache(uri, var_name, temporal, spatial)        

        shutil.copyfile('./test1.nc', cached.local_path)

        with self.assertNumQueries(1):
            cached, exists = self.task.check_cache(uri, var_name, temporal, spatial)        

        self.assertTrue(exists)

    def test_check_cache_file_missing(self):
        uri = './test1.nc'
        var_name = 'tas'
        temporal = slice(10, 20, 1)
        spatial = { 'latitude': (-45, 45), 'longitude': (-90, 90) }

        with self.assertNumQueries(5):
            self.task.check_cache(uri, var_name, temporal, spatial)        

        with self.assertNumQueries(1):
            cached, exists = self.task.check_cache(uri, var_name, temporal, spatial)        

        self.assertFalse(exists)

    def test_check_cache(self):
        uri = './test1.nc'
        var_name = 'tas'
        temporal = slice(10, 20, 1)
        spatial = { 'latitude': (-45, 45), 'longitude': (-90, 90) }

        with self.assertNumQueries(5):
            cached, exists = self.task.check_cache(uri, var_name, temporal, spatial)        

        uid = '126ba7a96e7d2a76608adafcc1da0e387a98eebf4b28c38805c36eb481682384'

        self.assertFalse(exists)
        self.assertEqual(cached.dimensions, 'time:10:20:1|latitude:-45:45:1|longitude:-90:90:1')
        self.assertEqual(cached.uid, uid)
        self.assertEqual(cached.url, './test1.nc')

    def test_generate_cache_name(self):
        file_name = './test1.nc'
        temporal = slice(10, 200, 1)
        spatial = { 'latitude': (-45, 45), 'longitude': (-90, 90) }

        name = self.task.generate_cache_name(file_name, temporal, spatial)

        self.assertEqual(name, 'dda6042243143d8193fc902bd9ceeef19219d6b55cde3c84eed469ae18bd9e7e')

    def test_generate_grid_from_domain(self):
        gridder = cwt.Gridder(grid='d0')

        domains = { 'd0': cwt.Domain([
            cwt.Dimension('latitude', -90, 180, step=1),
            cwt.Dimension('longitude', -180, 360, step=1)
        ])}

        operation = cwt.Process(identifier='CDAT.subset')

        operation.parameters['gridder'] = gridder

        grid, tool, method = self.task.generate_grid(operation, {}, domains)

        self.assertEqual(grid.shape, (270, 540))

    def test_generate_grid_from_file(self):
        gridder = cwt.Gridder(grid='v0')

        variables = { 'v0': cwt.Variable('./test1.nc', 'tas') }

        operation = cwt.Process(identifier='CDAT.subset')

        operation.parameters['gridder'] = gridder

        grid, tool, method = self.task.generate_grid(operation, variables, {})

        self.assertEqual(grid.shape, (180, 360))

    def test_generate_grid_uniform(self):
        gridder = cwt.Gridder(grid='uniform~4x3')

        operation = cwt.Process(identifier='CDAT.subset')

        operation.parameters['gridder'] = gridder

        grid, tool, method = self.task.generate_grid(operation, {}, {})

        self.assertEqual(grid.shape, (45, 120))

    def test_generate_grid_gaussian(self):
        gridder = cwt.Gridder(grid='gaussian~32')

        operation = cwt.Process(identifier='CDAT.subset')

        operation.parameters['gridder'] = gridder

        grid, tool, method = self.task.generate_grid(operation, {}, {})

        self.assertEqual(grid.shape, (32, 64))

    def test_map_domain_multiple_partial(self):
        domain = cwt.Domain([
            cwt.Dimension('time', 5, 10),
        ])

        with cdms2.open('./test1.nc') as infile1, cdms2.open('./test2.nc') as infile2:
            domain_map = self.task.map_domain_multiple([infile1, infile2], 'tas', domain)

        test1 = [x for x in domain_map.keys() if 'test1' in x][0]
        test2 = [x for x in domain_map.keys() if 'test2' in x][0]

        temporal1, spatial1 = domain_map[test1]

        self.assertEqual(temporal1, slice(5, 11, 1))

        temporal2, spatial2 = domain_map[test2]

        self.assertEqual(temporal2, None)

    def test_map_domain_multiple_whole(self):
        with cdms2.open('./test1.nc') as infile1, cdms2.open('./test2.nc') as infile2:
            domain_map = self.task.map_domain_multiple([infile1, infile2], 'tas', None)

        for v in domain_map.values():
            self.assertEqual(v[0], slice(0, 24, 1))

    def test_map_domain_multiple(self):
        domain = cwt.Domain([
            cwt.Dimension('time', 5, 35),
            cwt.Dimension('lat', -45, 45),
        ])

        with cdms2.open('./test1.nc') as infile1, cdms2.open('./test2.nc') as infile2:
            domain_map = self.task.map_domain_multiple([infile1, infile2], 'tas', domain)

        test1 = [x for x in domain_map.keys() if 'test1' in x][0]
        test2 = [x for x in domain_map.keys() if 'test2' in x][0]

        temporal1, spatial1 = domain_map[test1]

        self.assertEqual(temporal1, slice(5, 24, 1))
        self.assertDictEqual(spatial1, { 'latitude': (-45, 45) })

        temporal2, spatial2 = domain_map[test2]

        self.assertEqual(temporal2, slice(0, 12, 1))
        self.assertDictEqual(spatial1, { 'latitude': (-45, 45) })

    def test_map_domain_dimension_does_not_exist(self):
        domain = cwt.Domain([
            cwt.Dimension('level', 5, 20),
        ])

        with cdms2.open('./test1.nc') as infile, self.assertRaises(Exception):
            temporal, spatial = self.task.map_domain(infile, 'tas', domain)

    def test_map_domain_whole(self):
        with cdms2.open('./test1.nc') as infile:
            temporal, spatial = self.task.map_domain(infile, 'tas', None)

        self.assertEqual(temporal, slice(0, 24, 1))
        self.assertDictEqual(spatial, {})

    def test_map_domain(self):
        domain = cwt.Domain([
            cwt.Dimension('time', 5, 20),
            cwt.Dimension('lat', -45, 45),
            cwt.Dimension('lon', -90, 90)
        ])

        with cdms2.open('./test1.nc') as infile:
            temporal, spatial = self.task.map_domain(infile, 'tas', domain)

        self.assertEqual(temporal, slice(5, 21, 1))
        self.assertDictEqual(spatial, { 'latitude': (-45, 45), 'longitude': (-90, 90) })

    def test_map_time_unknown(self):
        dimension = cwt.Dimension('time', 0, 10, crs=cwt.CRS('custom'))

        with self.assertRaises(Exception):
            self.task.map_time_axis(self.time1, dimension)

    def test_map_time_axis_timestamps(self):
        dimension = cwt.Dimension('time', '1990-2-1', '1991-5-1', crs=cwt.CRS('timestamps'))

        axis = self.task.map_time_axis(self.time1, dimension)

        self.assertEqual(axis, slice(1, 17, 1))

    def test_map_time_axis_indices(self):
        dimension = cwt.Dimension('time', 5, 15, crs=cwt.INDICES)

        axis = self.task.map_time_axis(self.time1, dimension)

        self.assertEqual(axis, slice(5, 15, 1))

    def test_map_time_axis(self):
        dimension = cwt.Dimension('time', 5, 15)

        axis = self.task.map_time_axis(self.time1, dimension)

        self.assertEqual(axis, slice(5, 16, 1))

    def test_slice_to_string(self):
        self.assertEqual(self.task.slice_to_str(slice(10, 200, 1)), '10:200:1')

    def test_generate_output_local(self):
        output = self.task.generate_output('file:///test.nc', local=True)

        self.assertEqual('file:///test.nc', output)

    def test_generate_output_dap(self):
        settings.DAP = True

        output = self.task.generate_output('file:///test.nc')

        self.assertEqual(settings.DAP_URL.format(file_name='test.nc'), output)

        settings.DAP = False

    def test_generate_output(self):
        output = self.task.generate_output('file:///test.nc')

        self.assertRegexpMatches(output, '.*/thredds/dodsC/test/public/test.nc')

    def test_generate_local_output_custom_name(self):
        output = self.task.generate_local_output('test')

        self.assertEqual(output, '{}/test.nc'.format(settings.OUTPUT_LOCAL_PATH))

    def test_generate_local_output(self):
        output = self.task.generate_local_output()

        self.assertRegexpMatches(output, '{}/.*\.nc'.format(settings.OUTPUT_LOCAL_PATH))

    def test_op_by_id_missing(self):
        with self.assertRaises(Exception):
            self.task.op_by_id('CDAT.subset', {})

    def test_op_by_id(self):
        operations = { 'subset': cwt.Process(identifier='CDAT.subset') }

        self.task.op_by_id('CDAT.subset', operations)

    def test_load_missing_variable(self):
        variables = {} 
        domains = {'d0': { 'id': 'd0', 'time': { 'id': 'time', 'start': 0, 'end': 100, 'crs': 'values' }}}
        operations = { 'subset': { 'name': 'CDAT.subset', 'input': ['v0'], 'domain': 'd0'}}

        with self.assertRaises(cwt.ProcessError):
            v, d, o = self.task.load(variables, domains, operations)

    def test_load_missing_domain(self):
        variables = {'v0': { 'id': 'tas|v0', 'uri': 'file://./test.nc' }}
        domains = {}
        operations = { 'subset': { 'name': 'CDAT.subset', 'input': ['v0'], 'domain': 'd0'}}

        with self.assertRaises(Exception):
            v, d, o = self.task.load(variables, domains, operations)

    def test_load(self):
        variables = {'v0': { 'id': 'tas|v0', 'uri': 'file://./test.nc' }}
        domains = {'d0': { 'id': 'd0', 'time': { 'id': 'time', 'start': 0, 'end': 100, 'crs': 'values' }}}
        operations = { 'subset': { 'name': 'CDAT.subset', 'input': ['v0'], 'domain': 'd0'}}

        v, d, o = self.task.load(variables, domains, operations)

        self.assertEqual(len(o['subset'].inputs), 1)
        self.assertIsNotNone(o['subset'].domain)
        self.assertIsInstance(o['subset'].domain, cwt.Domain)

    def test_set_user_cred(self):
        self.task.set_user_creds(cwd='/users', user_id=self.user.pk)

        user_path = os.path.join('/', 'users', str(self.user.pk))

        self.assertTrue(os.path.exists(user_path))
        self.assertTrue(os.path.exists(os.path.join(user_path, '.dodsrc')))
        self.assertTrue(os.path.exists(os.path.join(user_path, 'creds.pem')))

    def test_get_job_missing_key(self):
        with self.assertRaises(Exception):
            self.task.get_job()

    def test_get_job_does_not_exist(self):
        with self.assertRaises(models.Job.DoesNotExist):
            self.task.get_job({'job_id': 10})

    def test_get_job(self):
        with self.assertNumQueries(1):
            job = self.task.get_job({'job_id': self.job.pk})

        self.assertIsInstance(job, models.Job)

    def test_initialize(self):
        with self.assertNumQueries(4):
            job, status = self.task.initialize(credentials=False, job_id=self.job.pk)

        self.assertIsInstance(job, models.Job)
        self.assertIsInstance(status, processes.Status)

        self.assertQuerysetEqual(self.job.status_set.all(), ['ProcessAccepted', 'ProcessStarted'], lambda x: x.status, ordered=False)
        self.assertQuerysetEqual(self.job.status_set.latest('created_date').message_set.all(), ['Job Started'], lambda x: x.message, ordered=False)
