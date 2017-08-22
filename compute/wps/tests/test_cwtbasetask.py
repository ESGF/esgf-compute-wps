import os
import re

import cdms2
import cwt
import numpy as np
from django import test
from django.db.models import Count

from wps import models
from wps import processes
from wps import settings

class CWTBaseTaskTestCase(test.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.user = models.User.objects.create(username='test')

        models.Auth.objects.create(user=cls.user, cert='test')

        server = models.Server.objects.create(host='test', status=0)

        cls.job = models.Job.objects.create(server=server, user=cls.user)

        cls.longitude = cdms2.createUniformLongitudeAxis(-180.0, 360.0, 1.0)

        cls.latitude = cdms2.createUniformLatitudeAxis(-90.0, 180.0, 1.0)

        cls.time1 = cdms2.createAxis(np.array([x for x in xrange(24)]))
        cls.time1.designateTime()
        cls.time1.units = 'months since 1990-1-1'

        with cdms2.open('./test1.nc', 'w') as outfile:
            outfile.write(
                np.array([[[10 for _ in xrange(360)] for _ in xrange(180)] for _ in xrange(24)]),
                axes=(cls.time1, cls.latitude, cls.longitude),
                id='tas'
            )

        cls.time2 = cdms2.createAxis(np.array([x for x in xrange(24)]))
        cls.time2.designateTime()
        cls.time2.units = 'months since 1992-1-1'

        with cdms2.open('./test2.nc', 'w') as outfile:
            outfile.write(
                np.array([[[10 for _ in xrange(360)] for _ in xrange(180)] for _ in xrange(24)]),
                axes=(cls.time2, cls.latitude, cls.longitude),
                id='tas'
            )

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.task = processes.CWTBaseTask()

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

        self.assertEqual(settings.OUTPUT_URL.format(file_name='test.nc'), output)

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

    def test_track_files_increment_requested(self):
        variables = {
            'test1': cwt.Variable('http://test1.com/test1.nc', 'tas'),
        }

        for _ in range(2):
            self.task.track_files(variables)

        qs = models.Files.objects.get(name='test1.nc')

        self.assertEqual(qs.requested, 2)

    def test_track_files(self):
        variables = {
            'test1': cwt.Variable('http://test1.com/test1.nc', 'tas'),
            'test2': cwt.Variable('http://test1.com/test2.nc', 'tas'),
            'test3': cwt.Variable('http://test2.com/test1.nc', 'tas'),
            'test4': cwt.Variable('http://test3.com/test3.nc', 'clt'),
        }

        self.assertNumQueries(8, self.task.track_files(variables))

        expected_names = ['test1.nc', 'test2.nc']

        filter_host_qs = models.Files.objects.filter(host='test1.com')

        self.assertQuerysetEqual(filter_host_qs, expected_names, lambda x: x.name, ordered=False)

        expected_hosts = ['test1.com', 'test2.com']

        filter_name_qs = models.Files.objects.filter(name='test1.nc')

        self.assertQuerysetEqual(filter_name_qs, expected_hosts, lambda x: x.host, ordered=False)

        expected_variables = ['tas', 'clt']

        group_variable_qs = models.Files.objects.values('variable').annotate(count=Count('variable'))

        self.assertQuerysetEqual(group_variable_qs, expected_variables, lambda x: x.values()[0], ordered=False)

    def test_initialize(self):
        with self.assertNumQueries(4):
            job, status = self.task.initialize(credentials=False, job_id=self.job.pk)

        self.assertIsInstance(job, models.Job)
        self.assertIsInstance(status, processes.Status)

        self.assertQuerysetEqual(self.job.status_set.all(), ['ProcessStarted'], lambda x: x.status, ordered=False)
        self.assertQuerysetEqual(self.job.status_set.latest('created_date').message_set.all(), ['Job Started'], lambda x: x.message, ordered=False)
