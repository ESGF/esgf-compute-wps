import datetime
import os
import random
import re
import shutil

import cdms2
import cwt
import mock
import numpy as np
from contextlib import nested
from django import test
from django.db.models import Count
from OpenSSL import crypto, SSL

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
        from socket import gethostname

        k = crypto.PKey()
        k.generate_key(crypto.TYPE_RSA, 2048)

        cert = crypto.X509()
        cert.get_subject().C = "US"
        cert.get_subject().ST = "CA"
        cert.get_subject().O = "test"
        cert.get_subject().OU = "test"
        cert.get_subject().CN = gethostname()
        cert.set_serial_number(1000)
        cert.gmtime_adj_notBefore(0)
        cert.gmtime_adj_notAfter(10*365*24*60*60)  # 10 years expiry date
        cert.set_issuer(cert.get_subject())  # self-sign this certificate

        cert.set_pubkey(k)
        cert.sign(k, 'sha256')

        cert_text = crypto.dump_certificate(crypto.FILETYPE_PEM, cert)
        key_text = crypto.dump_privatekey(crypto.FILETYPE_PEM, k)

        cls.cert = ''.join([cert_text, key_text])

        k = crypto.PKey()
        k.generate_key(crypto.TYPE_RSA, 2048)

        cert = crypto.X509()
        cert.get_subject().C = "US"
        cert.get_subject().ST = "CA"
        cert.get_subject().O = "test"
        cert.get_subject().OU = "test"
        cert.get_subject().CN = gethostname()
        cert.set_serial_number(1000)
        cert.gmtime_adj_notBefore(0)
        cert.gmtime_adj_notAfter(-10)  # 10 years expiry date
        cert.set_issuer(cert.get_subject())  # self-sign this certificate

        cert.set_pubkey(k)
        cert.sign(k, 'sha256')

        cert_text = crypto.dump_certificate(crypto.FILETYPE_PEM, cert)
        key_text = crypto.dump_privatekey(crypto.FILETYPE_PEM, k)

        cls.cert_expired = ''.join([cert_text, key_text])

        if os.path.exists(settings.CACHE_PATH):
            shutil.rmtree(settings.CACHE_PATH)

        os.makedirs(settings.CACHE_PATH)

        if os.path.exists(settings.LOCAL_OUTPUT_PATH):
            shutil.rmtree(settings.LOCAL_OUTPUT_PATH)

        os.makedirs(settings.LOCAL_OUTPUT_PATH)

        cls.longitude = cdms2.createUniformLongitudeAxis(-180.0, 360.0, 1.0)

        cls.latitude = cdms2.createUniformLatitudeAxis(-90.0, 180.0, 1.0)

        random.seed(1987)

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

        shutil.rmtree(settings.LOCAL_OUTPUT_PATH)

        try:
            os.remove('./test1.nc')
        except:
            pass

        try:
            os.remove('./test2.nc')
        except:
            pass

    def setUp(self):
        self.task = processes.CWTBaseTask()

        self.user = models.User.objects.create(username='test')

        models.Auth.objects.create(user=self.user, cert=self.cert)

        server = models.Server.objects.create(host='test', status=0)

        process = models.Process.objects.create(identifier='CDAT.subset', backend='local')

        self.job = models.Job.objects.create(server=server, user=self.user, process=process)

        self.job.accepted()

    def test_map_domain_timestamps(self):
        os.chdir(os.path.join(os.path.dirname(__file__), '..'))

        file_handles = [cdms2.open('./test1.nc'), cdms2.open('./test2.nc')]

        var_map = dict((x.id, 'tas') for x in file_handles)

        domain = cwt.Domain([
            cwt.Dimension('time', '1990-1-1 00:00:00.0', '1993-1-1 00:00:00.0', cwt.CRS('timestamps')),
        ])

        domain_map = self.task.map_domain(file_handles, var_map, domain)

        file_path = file_handles[0].id

        self.assertIn(file_path, domain_map)
        self.assertEqual(domain_map[file_path][0], slice(0, 24, 1))
        self.assertEqual(domain_map[file_path][1], {})

        file_path = file_handles[1].id

        self.assertIn(file_path, domain_map)
        self.assertEqual(domain_map[file_path][0], slice(0, 13, 1))
        self.assertEqual(domain_map[file_path][1], {})

    def test_map_domain_indices(self):
        os.chdir(os.path.join(os.path.dirname(__file__), '..'))

        file_handles = [cdms2.open('./test1.nc'), cdms2.open('./test2.nc')]

        var_map = dict((x.id, 'tas') for x in file_handles)

        domain = cwt.Domain([
            cwt.Dimension('time', 10, 40, cwt.INDICES),
        ])

        domain_map = self.task.map_domain(file_handles, var_map, domain)

        file_path = file_handles[0].id

        self.assertIn(file_path, domain_map)
        self.assertEqual(domain_map[file_path][0], slice(10, 24, 1))
        self.assertEqual(domain_map[file_path][1], {})

        file_path = file_handles[1].id

        self.assertIn(file_path, domain_map)
        self.assertEqual(domain_map[file_path][0], slice(0, 16, 1))
        self.assertEqual(domain_map[file_path][1], {})

    def test_map_domain_multiple(self):
        os.chdir(os.path.join(os.path.dirname(__file__), '..'))

        file_handles = [cdms2.open('./test1.nc'), cdms2.open('./test2.nc')]

        var_map = dict((x.id, 'tas') for x in file_handles)

        domain = cwt.Domain([
            cwt.Dimension('time', 10, 40),
        ])

        domain_map = self.task.map_domain(file_handles, var_map, domain)

        file_path = file_handles[0].id

        self.assertIn(file_path, domain_map)
        self.assertEqual(domain_map[file_path][0], slice(10, 24, 1))
        self.assertEqual(domain_map[file_path][1], {})

        file_path = file_handles[1].id

        self.assertIn(file_path, domain_map)
        self.assertEqual(domain_map[file_path][0], slice(0, 17, 1))
        self.assertEqual(domain_map[file_path][1], {})

    def test_map_domain(self):
        os.chdir(os.path.join(os.path.dirname(__file__), '..'))

        file_handles = [cdms2.open('./test1.nc'), cdms2.open('./test2.nc')]

        var_map = dict((x.id, 'tas') for x in file_handles)

        domain = cwt.Domain([
            cwt.Dimension('time', 10, 20),
        ])

        domain_map = self.task.map_domain(file_handles, var_map, domain)

        file_path = file_handles[0].id

        self.assertIn(file_path, domain_map)
        self.assertEqual(domain_map[file_path][0], slice(10, 21, 1))
        self.assertEqual(domain_map[file_path][1], {})

    def test_generate_partitions(self):
        domain_map = {
            'test1.nc': (slice(100, 1000, 1), {}),
        }

        partition_map = self.task.generate_partitions(domain_map)

        self.assertEqual(len(partition_map['test1.nc'][0]), 5)

    def test_retrieve_variable_cached(self):
        os.chdir(os.path.join(os.path.dirname(__file__), '..'))

        files = [
            cwt.Variable(os.path.abspath('./test1.nc'), 'tas'),
            cwt.Variable(os.path.abspath('./test2.nc'), 'tas'),
        ]

        domain = cwt.Domain([
            cwt.Dimension('time', 0, 40),
        ])

        self.job.started()

        cached, exists = self.task.check_cache(files[0].uri, 'tas', slice(0, 24, 1), {})

        shutil.copyfile('./test1.nc', cached.local_path)

        with self.assertNumQueries(34):
            output_path = self.task.retrieve_variable(files, domain, self.job)

        self.assertRegexpMatches(output_path, '/data/public/.*\.nc')

        with cdms2.open(output_path) as testfile:
            self.assertEqual(testfile['tas'].getTime().shape[0], 41)

    def test_retrieve_variable_partial(self):
        os.chdir(os.path.join(os.path.dirname(__file__), '..'))

        files = [
            cwt.Variable(os.path.abspath('./test1.nc'), 'tas'),
            cwt.Variable(os.path.abspath('./test2.nc'), 'tas'),
        ]

        domain = cwt.Domain([
            cwt.Dimension('time', 10, 20),
        ])

        self.job.started()

        with self.assertNumQueries(24):
            output_path = self.task.retrieve_variable(files, domain, self.job)

        self.assertRegexpMatches(output_path, '/data/public/.*\.nc')

        with cdms2.open(output_path) as testfile:
            self.assertEqual(testfile['tas'].getTime().shape[0], 11)

    def test_retrieve_variable_multiple(self):
        os.chdir(os.path.join(os.path.dirname(__file__), '..'))

        files = [
            cwt.Variable(os.path.abspath('./test1.nc'), 'tas'),
            cwt.Variable(os.path.abspath('./test2.nc'), 'tas'),
        ]

        domain = cwt.Domain([
            cwt.Dimension('time', 10, 40),
        ])

        self.job.started()

        with self.assertNumQueries(39):
            output_path = self.task.retrieve_variable(files, domain, self.job)

        self.assertRegexpMatches(output_path, '/data/public/.*\.nc')

        with cdms2.open(output_path) as testfile:
            self.assertEqual(testfile['tas'].getTime().shape[0], 31)

    def test_retrieve_variable_access_error(self):
        os.chdir(os.path.join(os.path.dirname(__file__), '..'))

        files = [
            cwt.Variable(os.path.abspath('./test12.nc'), 'tas')
        ]

        domain = cwt.Domain([
            cwt.Dimension('time', 10, 20)
        ])

        self.job.started()

        with self.assertNumQueries(0), self.assertRaises(processes.AccessError):
            self.task.retrieve_variable(files, domain, self.job)

    def test_retrieve_variable(self):
        os.chdir(os.path.join(os.path.dirname(__file__), '..'))

        files = [
            cwt.Variable(os.path.abspath('./test1.nc'), 'tas')
        ]

        domain = cwt.Domain([
            cwt.Dimension('time', 10, 20)
        ])

        self.job.started()

        with self.assertNumQueries(24):
            output_path = self.task.retrieve_variable(files, domain, self.job)

        self.assertRegexpMatches(output_path, '/data/public/.*\.nc')

        with cdms2.open(output_path) as testfile:
            self.assertEqual(testfile['tas'].getTime().shape[0], 11)

    def test_download_with_post_process(self):
        var_name = 'tas'
        base_units = 'months since 1800-1-1 1'
        temporal = (slice(0, 4, 1), slice(4, 8, 1))
        spatial = {}

        files = [
            cdms2.open('./test1.nc'),
            cdms2.open('./output.nc', 'w'),
            cdms2.open('./cache.nc', 'w')
        ]

        def post_process(data):
            return data + 10

        self.job.started()

        with nested(*files) as (infile, outfile, cachefile):
            self.task.download(infile, var_name, base_units, temporal, spatial, cachefile, outfile, post_process, self.job)

        files = [
            cdms2.open('./test1.nc'),
            cdms2.open('./output.nc'),
            cdms2.open('./cache.nc')
        ]

        with nested(*files) as (infile, outfile, cachefile):
            self.assertFalse((infile['tas'][0]==outfile['tas'][0]).all())
            self.assertTrue(((infile['tas'][0]+10)==outfile['tas'][0]).all())
            self.assertTrue((infile['tas'][0]==cachefile['tas'][0]).all())

        os.remove('./output.nc')
        os.remove('./cache.nc')

    def test_download_with_cache(self):
        var_name = 'tas'
        base_units = 'months since 1800-1-1 1'
        temporal = (slice(0, 4, 1), slice(4, 8, 1))
        spatial = {}

        files = [
            cdms2.open('./test1.nc'),
            cdms2.open('./output.nc', 'w'),
            cdms2.open('./cache.nc', 'w')
        ]

        self.job.started()

        with nested(*files) as (infile, outfile, cachefile):
            self.task.download(infile, var_name, base_units, temporal, spatial, cachefile, outfile, None, self.job)

        with cdms2.open('./output.nc') as testfile:
            self.assertEqual(testfile['tas'].getTime().shape[0], 8)

            self.assertEqual(testfile['tas'].getTime().units, 'months since 1800-1-1 1')

        with cdms2.open('./cache.nc') as testfile:
            self.assertEqual(testfile['tas'].getTime().shape[0], 8)

            self.assertEqual(testfile['tas'].getTime().units, 'months since 1990-1-1')

        os.remove('./output.nc')
        os.remove('./cache.nc')

    def test_download_with_cache(self):
        var_name = 'tas'
        base_units = 'months since 1800-1-1 1'
        temporal = (slice(0, 4, 1), slice(4, 8, 1))
        spatial = {}

        files = [
            cdms2.open('./test1.nc'),
            cdms2.open('./output.nc', 'w'),
        ]

        self.job.started()

        with nested(*files) as (infile, outfile):
            self.task.download(infile, var_name, base_units, temporal, spatial, None, outfile, None, self.job)

        with cdms2.open('./output.nc') as testfile:
            self.assertEqual(testfile['tas'].getTime().shape[0], 8)

            self.assertEqual(testfile['tas'].getTime().units, 'months since 1800-1-1 1')

        os.remove('./output.nc')

    def test_generate_cache_map_not_in_domain(self):
        file_handles = [cdms2.open('./test1.nc'), cdms2.open('./test2.nc')]

        time_slice = slice(0, 24, 1)

        cached, exists = self.task.check_cache(file_handles[0].id, 'tas', time_slice, {})

        shutil.copyfile('./test1.nc', cached.local_path)

        with nested(*file_handles):
            file_map = dict((x.id, x) for x in file_handles)

            var_map = dict((x.id, 'tas') for x in file_handles)

            domain_map = {
                file_handles[0].id: (time_slice, {}),
                file_handles[1].id: (None, {})
            }

            self.job.started()

            with self.assertNumQueries(4):
                cache_map = self.task.generate_cache_map(file_map, var_map, domain_map, self.job)

        self.assertNotIn(file_handles[1].id, file_map)

    def test_generate_cache_map_exists(self):
        file_handles = [cdms2.open('./test1.nc'), cdms2.open('./test2.nc')]

        time_slice = slice(0, 24, 1)

        cached, exists = self.task.check_cache(file_handles[0].id, 'tas', time_slice, {})

        shutil.copyfile('./test1.nc', cached.local_path)

        with nested(*file_handles):
            file_map = dict((x.id, x) for x in file_handles)

            var_map = dict((x.id, 'tas') for x in file_handles)

            domain_map = {
                file_handles[0].id: (time_slice, {})
            }

            self.job.started()

            with self.assertNumQueries(4):
                cache_map = self.task.generate_cache_map(file_map, var_map, domain_map, self.job)

        file_path = file_handles[0].id

        self.assertNotIn(file_path, cache_map)

    def test_generate_cache_map_multiple(self):
        file_handles = [cdms2.open('./test1.nc'), cdms2.open('./test2.nc')]

        with nested(*file_handles):
            file_map = dict((x.id, x) for x in file_handles)

            var_map = dict((x.id, 'tas') for x in file_handles)

            domain_map = {
                file_handles[0].id: (slice(0, 24, 1), {}),
                file_handles[1].id: (slice(0, 24, 1), {})
            }

            self.job.started()

            with self.assertNumQueries(13):
                cache_map = self.task.generate_cache_map(file_map, var_map, domain_map, self.job)

        file_path = file_handles[0].id

        self.assertIn(file_path, cache_map)

        file_path = file_handles[1].id

        self.assertIn(file_path, cache_map)

    def test_generate_cache_map_single(self):
        file_handles = [cdms2.open('./test1.nc'), cdms2.open('./test2.nc')]

        with nested(*file_handles):
            file_map = dict((x.id, x) for x in file_handles)

            var_map = dict((x.id, 'tas') for x in file_handles)

            domain_map = {
                file_handles[0].id: (slice(0, 24, 1), {})
            }

            self.job.started()

            with self.assertNumQueries(8):
                cache_map = self.task.generate_cache_map(file_map, var_map, domain_map, self.job)

        file_path = file_handles[0].id

        self.assertIn(file_path, cache_map)

    def test_generate_output_file(self):
        settings.DAP = False

        output_url = self.task.generate_output_url('/data/test.nc')

        self.assertEqual(output_url, 'http://0.0.0.0:8000/wps/output/test.nc')

    def test_generate_output_dap(self):
        settings.DAP = True

        output_url = self.task.generate_output_url('/data/test.nc')

        self.assertEqual(output_url, 'http://thredds:8080/thredds/dodsC/test/public/test.nc')

    def test_generate_output_url(self):
        output_url = self.task.generate_output_url('/data/test', local=True)

        self.assertEqual(output_url, 'file:///data/test')

    def test_generate_output_url(self):
        output_url = self.task.generate_output_url('file:///data/test', local=True)

        self.assertEqual(output_url, 'file:///data/test')

    def test_generate_output_path_name(self):
        output_path = self.task.generate_output_path('test')

        self.assertRegexpMatches(output_path, '/data/public/test.nc')

    def test_generate_output_path(self):
        output_path = self.task.generate_output_path()

        self.assertRegexpMatches(output_path, '/data/public/.*\.nc')

    def test_load_certificate(self):
        self.user.auth.type = 'oauth2'

        self.user.auth.cert = self.cert

        self.user.auth.save()

        self.task.load_certificate(self.user)

        self.assertTrue(os.path.exists('/tmp/{}/.dodsrc'.format(self.user.id)))

        self.assertTrue(os.path.exists('/tmp/{}/cert.pem'.format(self.user.id)))

        shutil.rmtree('/tmp/{}'.format(self.user.id))

    def test_refresh_certificate_mpc(self):
        self.user.auth.type = 'mpc'
        
        self.user.auth.save()

        with self.assertRaises(Exception):
            self.task.refresh_certificate(self.user)

    def test_refresh_certificate_oauth2(self):
        with self.assertRaises(Exception):
            self.task.refresh_certificate(self.user)

    def test_check_certificate_corrupt(self):
        self.user.auth.cert = 'test'
        
        self.user.auth.save()

        with self.assertNumQueries(0), self.assertRaises(Exception):
            self.assertFalse(self.task.check_certificate(self.user))

    def test_check_certificate_expired(self):
        self.user.auth.cert = self.cert_expired
        
        self.user.auth.save()

        with self.assertNumQueries(0):
            self.assertFalse(self.task.check_certificate(self.user))

    def test_check_certificate(self):
        with self.assertNumQueries(0):
            self.assertTrue(self.task.check_certificate(self.user))

    def test_initialize_credentials(self):
        with self.assertNumQueries(3):
            user, job = self.task.initialize(user_id=self.user.id, job_id=self.job.id, credentials=True)

        self.assertEqual(self.user, user)
        self.assertEqual(self.job, job)

    def test_initialize(self):
        with self.assertNumQueries(2):
            user, job = self.task.initialize(user_id=self.user.id, job_id=self.job.id)

        self.assertEqual(self.user, user)
        self.assertEqual(self.job, job)

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

    def test_get_job_missing_key(self):
        with self.assertRaises(Exception):
            self.task.get_job()

    def test_get_job_does_not_exist(self):
        with self.assertRaises(Exception):
            self.task.get_job({'job_id': 10})

    def test_get_job(self):
        with self.assertNumQueries(1):
            job = self.task.get_job({'job_id': self.job.pk})

        self.assertIsInstance(job, models.Job)
