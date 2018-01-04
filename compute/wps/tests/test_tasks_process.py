#! /usr/bin/env python

import datetime
import mock
import time
from django import test
from django.utils import timezone

import cdms2
import cwt
from cdms2 import MV2 as MV
from openid.consumer import discover

from . import helpers
from wps import models
from wps import settings
from wps import WPSError
from wps.auth import openid
from wps.tasks import process

class ProcessTasksTestCase(test.TestCase):
    fixtures = ['users.json', 'servers.json']

    def setUp(self):
        self.user = models.User.objects.first()

        self.server = models.Server.objects.get(host='default')

        self.process = models.Process.objects.create(identifier='CDAT.test', server=self.server)

    @mock.patch('wps.tasks.process.cdms2.open')
    @mock.patch('wps.tasks.process.celery.Task.request')
    def test_cwt_task_process_variable(self, mock_request, mock_open):
        mock_request.id = 'uid'

        job = models.Job.objects.create(server=self.server, process=self.process, user=self.user)

        job.accepted()

        job.started()

        file1 = 'file:///test_1990-2000.nc'
        file2 = 'file:///test_1980-1990.nc'

        variables = [
            cwt.Variable(file1, 'tas'),
            cwt.Variable(file2, 'tas'),
        ]

        domain = cwt.Domain([
            cwt.Dimension('time', 100, 200),
            cwt.Dimension('lat', 90, 180),
            cwt.Dimension('lon', 180, 360),
        ])

        proc = MV.sum

        domain_map = {
            file1: (slice(100, 200), {'lat': slice(0, 180), 'lon': slice(0, 360)})
        }

        cache_map = {}

        task = process.CWTBaseTask()

        task.open_variables = mock.Mock(return_value=(mock.MagicMock(), mock.MagicMock()))

        task.map_domain = mock.Mock(return_value=domain_map)

        task.generate_cache_map = mock.Mock(return_value=cache_map)

        task.process_variable(variables, domain, job, proc, 1, axes=['lat'])

        mock_open.assert_called()

    def test_cwt_task_sort_variables_by_time_skip_file(self):
        task = process.CWTBaseTask()

        file_name1 = 'file:///test1_1990-2000.nc'

        file_name2 = 'file:///test.nc'

        variable_list = [cwt.Variable(file_name1, 'tas'), cwt.Variable(file_name2, 'tas')]
        
        with self.assertRaises(WPSError) as e:
            variables = task.sort_variables_by_time(variable_list)

    def test_cwt_task_sort_variables_by_time(self):
        task = process.CWTBaseTask()

        file_name1 = 'file:///test1_1990-2000.nc'

        file_name2 = 'file:///test2_1980-1990.nc'

        variable_list = [cwt.Variable(file_name1, 'tas'), cwt.Variable(file_name2, 'tas')]
        
        variables = task.sort_variables_by_time(variable_list)

        self.assertEqual(variables[0].uri, file_name2)
        self.assertEqual(variables[1].uri, file_name1)

    @mock.patch('wps.tasks.process.cdms2.open')
    def test_cwt_task_open_variables_access_error(self, mock_open):
        mock_open.side_effect = cdms2.CDMSError('error')

        task = process.CWTBaseTask()

        variable_list = [cwt.Variable('file:///test1.nc', 'tas')]

        with self.assertRaises(process.AccessError) as e:
            variables = task.open_variables(variable_list)

    @mock.patch('wps.tasks.process.cdms2.open')
    def test_cwt_task_open_variables_access_error_cleanup(self, mock_open):
        mock_data = mock.MagicMock()

        mock_open.side_effect = [mock_data, cdms2.CDMSError('error')]

        task = process.CWTBaseTask()

        variable_list = [cwt.Variable('file:///test1.nc', 'tas'), cwt.Variable('file:///test2.nc', 'tas')]

        with self.assertRaises(process.AccessError) as e:
            variables = task.open_variables(variable_list)

        mock_data.close.assert_called()

    @mock.patch('wps.tasks.process.cdms2.open')
    def test_cwt_task_open_variables(self, mock_open):
        mock_open.return_value = 'some data'        

        task = process.CWTBaseTask()

        file_path = 'file:///test1.nc'

        variable_list = [cwt.Variable(file_path, 'tas')]

        file_map, var_map = task.open_variables(variable_list)

        self.assertIn(file_path, file_map)
        self.assertEqual(file_map[file_path], 'some data')

        self.assertIn(file_path, var_map)
        self.assertEqual(var_map[file_path], 'tas')

    @mock.patch('wps.tasks.process.celery.Task.request')
    def test_cwt_task_download_invalid_shape(self, mock_request):
        mock_request.id = 'uid'

        job = models.Job.objects.create(server=self.server, process=self.process, user=self.user)

        job.accepted()

        job.started()

        task = process.CWTBaseTask()

        input_file = mock.MagicMock()

        input_file.return_value.shape = [0, 200, 100]

        kwargs = {
            'input_file': input_file,
            'var_name': 'tas',
            'base_units': 'days since 1990',
            'temporal': [slice(0, 100), slice(100, 200)],
            'spatial': {'lat': slice(0, 180), 'lon': slice(0, 360)},
            'cache_file': mock.MagicMock(),
            'out_file': mock.MagicMock(),
            'post_process': None,
            'job': job
        }

        with self.assertRaises(process.InvalidShapeError) as e:
            task.download(**kwargs)

    @mock.patch('wps.tasks.process.celery.Task.request')
    def test_cwt_task_download_not_caching(self, mock_request):
        mock_request.id = 'uid'

        job = models.Job.objects.create(server=self.server, process=self.process, user=self.user)

        job.accepted()

        job.started()

        task = process.CWTBaseTask()

        kwargs = {
            'input_file': mock.MagicMock(),
            'var_name': 'tas',
            'base_units': 'days since 1990',
            'temporal': [slice(0, 100), slice(100, 200)],
            'spatial': {'lat': slice(0, 180), 'lon': slice(0, 360)},
            'cache_file': None,
            'out_file': mock.MagicMock(),
            'post_process': None,
            'job': job
        }

        task.download(**kwargs)

    @mock.patch('wps.tasks.process.celery.Task.request')
    def test_cwt_task_download_rebase_time(self, mock_request):
        mock_request.id = 'uid'

        job = models.Job.objects.create(server=self.server, process=self.process, user=self.user)

        job.accepted()

        job.started()

        time = helpers.generate_time('days since 1990', 365)

        variable = helpers.generate_variable([time, helpers.longitude, helpers.latitude], 'tas')

        task = process.CWTBaseTask()

        kwargs = {
            'input_file': mock.MagicMock(return_value=variable),
            'var_name': 'tas',
            'base_units': 'days since 1980',
            'temporal': [slice(0, 100), slice(100, 200)],
            'spatial': {'lat': slice(0, 180), 'lon': slice(0, 360)},
            'cache_file': mock.MagicMock(),
            'out_file': mock.MagicMock(),
            'post_process': None,
            'job': job
        }

        task.download(**kwargs)

        self.assertEqual(variable.getTime().units, 'days since 1980')

    @mock.patch('wps.tasks.process.celery.Task.request')
    def test_cwt_task_download_post_process(self, mock_request):
        mock_request.id = 'uid'

        job = models.Job.objects.create(server=self.server, process=self.process, user=self.user)

        job.accepted()

        job.started()

        time = helpers.generate_time('days since 1990', 365)

        variable = helpers.generate_variable([time, helpers.longitude, helpers.latitude], 'tas')

        task = process.CWTBaseTask()

        post_process = mock.MagicMock()

        kwargs = {
            'input_file': mock.MagicMock(return_value=variable),
            'var_name': 'tas',
            'base_units': 'days since 1990',
            'temporal': [slice(0, 100), slice(100, 200)],
            'spatial': {'lat': slice(0, 180), 'lon': slice(0, 360)},
            'cache_file': mock.MagicMock(),
            'out_file': mock.MagicMock(),
            'post_process': post_process,
            'job': job
        }

        task.download(**kwargs)

        post_process.assert_called() 

    @mock.patch('wps.tasks.process.celery.Task.request')
    def test_cwt_task_download(self, mock_request):
        mock_request.id = 'uid'

        job = models.Job.objects.create(server=self.server, process=self.process, user=self.user)

        job.accepted()

        job.started()

        time = helpers.generate_time('days since 1990', 365)

        variable = helpers.generate_variable([time, helpers.longitude, helpers.latitude], 'tas')

        task = process.CWTBaseTask()

        kwargs = {
            'input_file': mock.MagicMock(return_value=variable),
            'var_name': 'tas',
            'base_units': 'days since 1990',
            'temporal': [slice(0, 100), slice(100, 200)],
            'spatial': {'lat': slice(0, 180), 'lon': slice(0, 360)},
            'cache_file': mock.MagicMock(),
            'out_file': mock.MagicMock(),
            'post_process': None,
            'job': job
        }

        task.download(**kwargs)

    def test_cwt_task_free_cache_free_single_oldest(self):
        settings.CACHE_GB_MAX_SIZE = 0.35

        size_list = [0.2, 0.1, 0.05, 0.05]

        for i, size in enumerate(size_list):
            models.Cache.objects.create(uid='uid', url='url', dimensions='', size=size, accessed_date=timezone.now()+timezone.timedelta(days=10-i))

        task = process.CWTBaseTask()

        with self.assertNumQueries(3):
            space = task.free_cache(0.1)

        self.assertEqual(space, 0.2)

    def test_cwt_task_free_cache_free_single(self):
        settings.CACHE_GB_MAX_SIZE = 0.35

        size_list = [0.2, 0.1, 0.05, 0.05]

        for i, size in enumerate(size_list):
            models.Cache.objects.create(uid='uid', url='url', dimensions='', size=size, accessed_date=timezone.now()+timezone.timedelta(days=10+i))

        task = process.CWTBaseTask()

        with self.assertNumQueries(4):
            space = task.free_cache(0.1)

        self.assertEqual(space, 0.1)

    def test_cwt_task_free_cache(self):
        size_list = [0.2, 0.1, 0.05, 0.05]

        for size in size_list:
            models.Cache.objects.create(uid='uid', url='url', dimensions='', size=size)

        task = process.CWTBaseTask()

        with self.assertNumQueries(1):
            space = task.free_cache(0.2)

        self.assertEqual(space, 0)

    @mock.patch('wps.tasks.process.celery.Task.request')
    def test_cwt_task_generate_cache_map_skip_file(self, mock_request):
        mock_request.id.return_value = 'uid'

        job = models.Job.objects.create(server=self.server, process=self.process, user=self.user)

        job.accepted()

        job.started()

        file_map = {'test1.nc': mock.MagicMock()}

        var_map = {'test1.nc': 'tas'}

        domain_map = {'test1.nc': (None, {'lat': slice(0, 180), 'lon': slice(0, 360)})}

        task = process.CWTBaseTask()

        caches = task.generate_cache_map(file_map, var_map, domain_map, job)

        self.assertDictEqual(caches, {})

    @mock.patch('wps.tasks.process.cdms2.open')
    @mock.patch('wps.tasks.process.celery.Task.request')
    def test_cwt_task_generate_cache_map_cached_error_opening(self, mock_request, mock_open):
        mock_file = mock_open.side_effect = cdms2.CDMSError('error')

        mock_request.id.return_value = 'uid'

        job = models.Job.objects.create(server=self.server, process=self.process, user=self.user)

        job.accepted()

        job.started()

        file_map = {'test1.nc': mock.MagicMock()}

        var_map = {'test1.nc': 'tas'}

        domain_map = {'test1.nc': (slice(100, 200), {'lat': slice(0, 180), 'lon': slice(0, 360)})}

        task = process.CWTBaseTask()

        task.check_cache = mock.Mock(return_value=(mock.MagicMock(), True))

        with self.assertRaises(process.AccessError) as e:
            caches = task.generate_cache_map(file_map, var_map, domain_map, job)

    @mock.patch('wps.tasks.process.celery.Task.request')
    def test_cwt_task_generate_cache_map_not_cached(self, mock_request):
        mock_request.id.return_value = 'uid'

        job = models.Job.objects.create(server=self.server, process=self.process, user=self.user)

        job.accepted()

        job.started()

        file_map = {'test1.nc': mock.MagicMock()}

        var_map = {'test1.nc': 'tas'}

        domain_map = {'test1.nc': (slice(100, 200), {'lat': slice(0, 180), 'lon': slice(0, 360)})}

        task = process.CWTBaseTask()

        task.check_cache = mock.Mock(return_value=(mock.MagicMock(**{'estimate_size.return_value': 200}), False))

        task.free_cache = mock.MagicMock()

        caches = task.generate_cache_map(file_map, var_map, domain_map, job)

        self.assertIn('test1.nc', caches)

        task.free_cache.assert_called_with(200)

    @mock.patch('wps.tasks.process.cdms2.open')
    @mock.patch('wps.tasks.process.celery.Task.request')
    def test_cwt_task_generate_cache_map_cached(self, mock_request, mock_open):
        mock_file = mock_open.return_value

        mock_file.__getitem__.return_value.getTime.return_value.shape = (200,)

        mock_request.id.return_value = 'uid'

        job = models.Job.objects.create(server=self.server, process=self.process, user=self.user)

        job.accepted()

        job.started()

        file_map = {'test1.nc': mock.MagicMock()}

        var_map = {'test1.nc': 'tas'}

        domain_map = {'test1.nc': (slice(100, 200), {'lat': slice(0, 180), 'lon': slice(0, 360)})}

        task = process.CWTBaseTask()

        task.check_cache = mock.Mock(return_value=(mock.MagicMock(), True))

        caches = task.generate_cache_map(file_map, var_map, domain_map, job)

        self.assertEqual(domain_map['test1.nc'][0], slice(0, 200, 1))
        self.assertDictEqual(caches, {})

    @mock.patch('wps.tasks.process.celery.Task.request')
    def test_cwt_task_generate_cache_map(self, mock_request):
        mock_request.id.return_value = 'uid'

        job = models.Job.objects.create(server=self.server, process=self.process, user=self.user)

        job.accepted()

        job.started()

        task = process.CWTBaseTask()

        caches = task.generate_cache_map({}, {}, {}, job)

        self.assertDictEqual(caches, {})

    def test_cwt_task_slice_to_str_list(self):
        task = process.CWTBaseTask()

        data = task.slice_to_str([100, 200])

        self.assertEqual(data, '100:200:1')

    def test_cwt_task_slice_to_str_step(self):
        task = process.CWTBaseTask()

        data = task.slice_to_str((100, 200, 2))

        self.assertEqual(data, '100:200:2')

    def test_cwt_task_slice_to_str_slice(self):
        task = process.CWTBaseTask()

        data = task.slice_to_str(slice(100, 200, 4))

        self.assertEqual(data, '100:200:4')

    def test_cwt_task_slice_to_str_unknown_type(self):
        task = process.CWTBaseTask()

        with self.assertRaises(WPSError) as e:
            task.slice_to_str('test')

    def test_cwt_task_slice_to_str(self):
        task = process.CWTBaseTask()

        data = task.slice_to_str((100, 200))

        self.assertEqual(data, '100:200:1')

    def test_cwt_task_generate_output_url_non_dap(self):
        settings.DAP = False

        task = process.CWTBaseTask()

        path = task.generate_output_url('file:///directory/test.nc')

        self.assertEqual(path, settings.OUTPUT_URL.format(file_name='test.nc'))

    def test_cwt_task_generate_output_url_local(self):
        task = process.CWTBaseTask()

        path = task.generate_output_url('file:///directory/test.nc', local=True)

        self.assertEqual(path, 'file:///directory/test.nc')

    def test_cwt_task_generate_output_url_local_no_protocol(self):
        task = process.CWTBaseTask()

        path = task.generate_output_url('/directory/test.nc', local=True)

        self.assertEqual(path, 'file:///directory/test.nc')

    def test_cwt_task_generate_output_url(self):
        task = process.CWTBaseTask()

        path = task.generate_output_url('file:///directory/test.nc')

        self.assertEqual(path, settings.DAP_URL.format(file_name='test.nc'))

    def test_cwt_task_generate_output_path(self):
        task = process.CWTBaseTask()

        path = task.generate_output_path('unique')

        self.assertEqual(path, '{}/unique.nc'.format(settings.LOCAL_OUTPUT_PATH))
    
    @mock.patch('wps.tasks.process.uuid.uuid4')
    def test_cwt_task_generate_output_path(self, mock_uuid4):
        mock_uuid4.return_value = 'uuid'

        task = process.CWTBaseTask()

        path = task.generate_output_path()

        self.assertEqual(path, '{}/uuid.nc'.format(settings.LOCAL_OUTPUT_PATH))

    def test_cwt_task_load_parent_list(self):
        task = process.CWTBaseTask()

        parent_variables = [{'v1': {'id': 'tas|v1', 'uri': 'file:///test2.nc'}}, {'v2': {'id': 'tas|v2', 'uri': 'file:///test3.nc'}}]
        variables = {'v0': {'id': 'tas|v0', 'uri': 'file:///test.nc'}}
        domains = {'d0': {'id': 'd0', 'time': {'start': 0, 'stop': 200, 'crs': 'values'}}}
        operation = {'name': 'CDAT.test', 'input': ['v0', 'v1'], 'domain': 'd0'}

        v, d, o = task.load(parent_variables, variables, domains, operation)

        self.assertIn('v0', v)
        self.assertIsInstance(v['v0'], cwt.Variable)

        self.assertIn('v1', v)
        self.assertIsInstance(v['v1'], cwt.Variable)

        self.assertIn('v2', v)
        self.assertIsInstance(v['v2'], cwt.Variable)

        self.assertIn('d0', d)
        self.assertIsInstance(d['d0'], cwt.Domain)

        self.assertIsInstance(o, cwt.Process)

    def test_cwt_task_load_parent_dict(self):
        task = process.CWTBaseTask()

        parent_variables = {'v1': {'id': 'tas|v1', 'uri': 'file:///test2.nc'}}
        variables = {'v0': {'id': 'tas|v0', 'uri': 'file:///test.nc'}}
        domains = {'d0': {'id': 'd0', 'time': {'start': 0, 'stop': 200, 'crs': 'values'}}}
        operation = {'name': 'CDAT.test', 'input': ['v0', 'v1'], 'domain': 'd0'}

        v, d, o = task.load(parent_variables, variables, domains, operation)

        self.assertIn('v0', v)
        self.assertIsInstance(v['v0'], cwt.Variable)

        self.assertIn('v1', v)
        self.assertIsInstance(v['v1'], cwt.Variable)

        self.assertIn('d0', d)
        self.assertIsInstance(d['d0'], cwt.Domain)

        self.assertIsInstance(o, cwt.Process)

    def test_cwt_task_load(self):
        task = process.CWTBaseTask()

        variables = {'v0': {'id': 'tas|v0', 'uri': 'file:///test.nc'}}
        domains = {'d0': {'id': 'd0', 'time': {'start': 0, 'stop': 200, 'crs': 'values'}}}
        operation = {'name': 'CDAT.test', 'input': ['v0'], 'domain': 'd0'}

        v, d, o = task.load(None, variables, domains, operation)

        self.assertIn('v0', v)
        self.assertIsInstance(v['v0'], cwt.Variable)

        self.assertIn('d0', d)
        self.assertIsInstance(d['d0'], cwt.Domain)

        self.assertIsInstance(o, cwt.Process)

    @mock.patch('wps.tasks.process.open')
    @mock.patch('wps.tasks.process.os')
    def test_cwt_task_load_certificate_refresh_certificate(self, mock_os, mock_open):
        task = process.CWTBaseTask()

        task.check_certificate = mock.Mock(return_value=False)

        task.refresh_certificate = mock.Mock()
        
        task.load_certificate(self.user)

        task.refresh_certificate.assert_called()

    @mock.patch('wps.tasks.process.open')
    @mock.patch('wps.tasks.process.os')
    def test_cwt_task_load_certificate_user_path_does_not_exist(self, mock_os, mock_open):
        mock_os.path.exists.return_value = False

        task = process.CWTBaseTask()

        task.check_certificate = mock.Mock(return_value=True)
        
        task.load_certificate(self.user)

        mock_os.makedirs.assert_called()

    @mock.patch('wps.tasks.process.open')
    @mock.patch('wps.tasks.process.os')
    def test_cwt_task_load_certificate(self, mock_os, mock_open):
        #TODO verify all the os calls
        task = process.CWTBaseTask()

        task.check_certificate = mock.Mock(return_value=True)
        
        task.load_certificate(self.user)

    def test_cwt_task_refresh_certificate_myproxyclient(self):
        self.user.auth.type = 'myproxyclient'

        self.user.auth.save()

        task = process.CWTBaseTask()

        with self.assertRaises(process.CertificateError):
            task.refresh_certificate(self.user)

    @mock.patch('wps.tasks.process.discover')
    def test_cwt_task_refresh_certificate_discovery_error(self, mock_discover):
        mock_discover.discoverYadis.side_effect = discover.DiscoveryFailure('url', 401)

        task = process.CWTBaseTask()

        with self.assertRaises(discover.DiscoveryFailure) as e:
            task.refresh_certificate(self.user)

    @mock.patch('wps.tasks.process.discover')
    def test_cwt_task_refresh_certificate_missing_oauth2_state(self, mock_discover):
        mock_discover.discoverYadis.return_value = ('url', {})

        task = process.CWTBaseTask()

        with self.assertRaises(WPSError) as e:
            task.refresh_certificate(self.user)

    @mock.patch('wps.tasks.process.discover')
    def test_cwt_task_refresh_certificate_missing_oauth2_token(self, mock_discover):
        self.user.auth.extra = '{}'

        self.user.auth.save()

        mock_discover.discoverYadis.return_value = ('url', {})

        task = process.CWTBaseTask()

        with self.assertRaises(WPSError) as e:
            task.refresh_certificate(self.user)

    @mock.patch('wps.tasks.process.openid')
    @mock.patch('wps.tasks.process.oauth2')
    @mock.patch('wps.tasks.process.discover')
    def test_cwt_task_refresh_certificate(self, mock_discover, mock_oauth2, mock_openid):
        mock_openid.find_service_by_type.side_effect = [mock.MagicMock(), mock.MagicMock()]

        mock_oauth2.get_certificate.return_value = ('cert', 'key', 'new_token')

        self.user.auth.extra = '{"token": "oauth2_token"}'

        self.user.auth.save()

        mock_discover.discoverYadis.return_value = ('url', {})

        task = process.CWTBaseTask()

        with self.assertNumQueries(1):
            data = task.refresh_certificate(self.user)

        self.assertEqual(data, 'certkey')

    def test_cwt_task_check_certificate_missing(self):
        task = process.CWTBaseTask()

        with self.assertRaises(process.CertificateError) as e:
            task.check_certificate(self.user)

    @mock.patch('wps.tasks.process.crypto')
    def test_cwt_task_check_certificate_not_before(self, mock_crypto):
        expired = datetime.datetime.now() + datetime.timedelta(days=10)

        mock_crypto.load_certificate.return_value.get_notBefore.return_value = expired.strftime(process.CERT_DATE_FMT)
        
        mock_crypto.load_certificate.return_value.get_notAfter.return_value = datetime.datetime.now().strftime(process.CERT_DATE_FMT)

        self.user.auth.cert = 'some certificate'

        self.user.auth.save()

        task = process.CWTBaseTask()

        result = task.check_certificate(self.user)

        self.assertFalse(result)

    @mock.patch('wps.tasks.process.crypto')
    def test_cwt_task_check_certificate_not_after(self, mock_crypto):
        expired = datetime.datetime.now() - datetime.timedelta(days=10)

        mock_crypto.load_certificate.return_value.get_notBefore.return_value = datetime.datetime.now().strftime(process.CERT_DATE_FMT)
        
        mock_crypto.load_certificate.return_value.get_notAfter.return_value = expired.strftime(process.CERT_DATE_FMT)

        self.user.auth.cert = 'some certificate'

        self.user.auth.save()

        task = process.CWTBaseTask()

        result = task.check_certificate(self.user)

        self.assertFalse(result)

    @mock.patch('wps.tasks.process.crypto')
    def test_cwt_task_check_certificate_fail_to_load(self, mock_crypto):
        mock_crypto.load_certificate.side_effect = Exception('Failed to load certificate')

        self.user.auth.cert = 'some certificate'

        self.user.auth.save()

        task = process.CWTBaseTask()

        with self.assertRaises(process.CertificateError) as e:
            result = task.check_certificate(self.user)

    @mock.patch('wps.tasks.process.crypto')
    def test_cwt_task_check_certificate(self, mock_crypto):
        not_before = datetime.datetime.now() - datetime.timedelta(days=10)

        not_after = datetime.datetime.now() + datetime.timedelta(days=10)

        mock_crypto.load_certificate.return_value.get_notBefore.return_value = not_before.strftime(process.CERT_DATE_FMT)
        
        mock_crypto.load_certificate.return_value.get_notAfter.return_value = not_after.strftime(process.CERT_DATE_FMT)

        self.user.auth.cert = 'some certificate'

        self.user.auth.save()

        task = process.CWTBaseTask()

        result = task.check_certificate(self.user)

        self.assertTrue(result)

    def test_cwt_task_get_job_does_not_exist(self):
        task = process.CWTBaseTask()

        with self.assertRaises(WPSError) as e:
            task.get_job({'job_id': 0})

    def test_cwt_task_get_job(self):
        job = models.Job.objects.create(server=self.server, process=self.process, user=self.user)

        task = process.CWTBaseTask()

        with self.assertNumQueries(1):
            job2 = task.get_job({'job_id': job.id})

        self.assertEqual(job, job2)

    @mock.patch('wps.tasks.process.celery.Task.request')
    def test_cwt_task_status(self, mock_request):
        mock_request.return_value = mock.Mock(**{'id.return_value': 'task_id'})

        job = models.Job.objects.create(server=self.server, process=self.process, user=self.user)

        job.accepted()

        job.started()
        
        task = process.CWTBaseTask()

        with self.assertNumQueries(3):
            task.status(job, 'Test Message', 58)
    
    def test_cwt_task_initialize_missing_user(self):
        task = process.CWTBaseTask()

        with self.assertRaises(WPSError) as e:
            task.initialize(user_id=0, job_id=0, credentials=False)

    def test_cwt_task_initialize_missing_job(self):
        user = models.User.objects.first()

        task = process.CWTBaseTask()

        with self.assertRaises(WPSError) as e:
            task.initialize(user_id=user.id, job_id=0, credentials=False)

    def test_cwt_task_initialize_with_credentials(self):
        job = models.Job.objects.create(server=self.server, process=self.process, user=self.user)

        task = process.CWTBaseTask()

        task.load_certificate = mock.MagicMock()

        task.initialize(user_id=self.user.id, job_id=job.id, credentials=True)

        task.load_certificate.assert_called_with(self.user)

    def test_cwt_task_initialize(self):
        job = models.Job.objects.create(server=self.server, process=self.process, user=self.user)

        task = process.CWTBaseTask()

        with self.assertNumQueries(2):
            user2, job2 = task.initialize(user_id=self.user.id, job_id=job.id, credentials=False)

        self.assertEqual(self.user, user2)
        self.assertEqual(job, job2)

    def test_int_or_float_cannot_parse(self):
        data = process.int_or_float('test')

        self.assertIsNone(data)

    def test_int_or_float_value_float(self):
        data = process.int_or_float('3.14')

        self.assertIsInstance(data, float)
        self.assertEqual(data, 3.14)

    def test_int_or_float_value_int(self):
        data = process.int_or_float('10')

        self.assertIsInstance(data, int)
        self.assertEqual(data, 10)

    def test_get_process_does_not_exist(self):
        with self.assertRaises(WPSError):
            process.get_process('CDAT.debug')

    def test_get_process(self):
        @process.register_process('CDAT.test')
        def test_task(self):
            pass

        proc = process.get_process('CDAT.test')

        self.assertEqual(proc, test_task)

    def test_register_process(self):
        @process.register_process('CDAT.test')
        def test_task(self):
            pass

        self.assertIn('CDAT.test', process.REGISTRY)
        self.assertEqual(process.REGISTRY['CDAT.test'], test_task)
