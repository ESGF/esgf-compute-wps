import cwt
import mock
from django import test

from wps import models
from wps import WPSError
from wps.tasks import base

class CWTBaseTaskTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'servers.json', 'jobs.json']


    def test_cwt_base_load_multiple_parent_variables(self):
        parent_variables = [
            {
                'v1': {'id': 'tas|v1', 'uri': 'file:///test1.nc'} 
            },
            {
                'v2': {'id': 'tas|v2', 'uri': 'file:///test2.nc'}
            }
        ]

        variables = {
            'v0': {'id': 'tas|v0', 'uri': 'file:///test.nc'}
        }

        domains = {
            'd0': {'id': 'd0', 'time': {'start': '20', 'end': '200', 'crs': 'values'}},
        }

        operation = {'name': 'CDAT.subset', 'input': ['v0'], 'domain': 'd0'}

        task = base.CWTBaseTask()

        v, d, o = task.load(parent_variables, variables, domains, operation) 

        self.assertEqual(len(v), 3)

    def test_cwt_base_load_parent_variables(self):
        parent_variables = {
            'v1': {'id': 'tas|v1', 'uri': 'file:///test1.nc'} 
        }

        variables = {
            'v0': {'id': 'tas|v0', 'uri': 'file:///test.nc'}
        }

        domains = {
            'd0': {'id': 'd0', 'time': {'start': '20', 'end': '200', 'crs': 'values'}},
        }

        operation = {'name': 'CDAT.subset', 'input': ['v0'], 'domain': 'd0'}

        task = base.CWTBaseTask()

        v, d, o = task.load(parent_variables, variables, domains, operation) 

        self.assertEqual(len(v), 2)

    def test_cwt_base_load(self):
        variables = {
            'v0': {'id': 'tas|v0', 'uri': 'file:///test.nc'}
        }

        domains = {
            'd0': {'id': 'd0', 'time': {'start': '20', 'end': '200', 'crs': 'values'}},
        }

        operation = {'name': 'CDAT.subset', 'input': ['v0'], 'domain': 'd0'}

        task = base.CWTBaseTask()

        v, d, o = task.load(None, variables, domains, operation) 

        self.assertIsInstance(v, dict)
        self.assertIn('v0', v)
        self.assertIsInstance(v['v0'], cwt.Variable)

        self.assertIsInstance(d, dict)
        self.assertIn('d0', d)
        self.assertIsInstance(d['d0'], cwt.Domain)

        self.assertIsInstance(o, cwt.Process)
        self.assertEqual(o.domain, d['d0'])
        self.assertEqual(len(o.inputs), 1)
        self.assertEqual(o.inputs[0], v['v0'])

    @mock.patch('wps.tasks.base.models.Job.objects.get')
    def test_cwt_base_task_missing_job(self, mock_get):
        job = models.Job.objects.first()

        mock_get.side_effect = models.Job.DoesNotExist()

        task = base.CWTBaseTask()

        task.PUBLISH = base.ALL

        task.on_success(None, 0, None, {})

    def test_cwt_base_task_missing_job_id(self):
        task = base.CWTBaseTask()

        task.PUBLISH = base.ALL

        task.on_success(None, 0, None, {})

    @mock.patch('wps.tasks.base.models.Job.objects.get')
    def test_cwt_base_task_success_cant_publish(self, mock_get):
        job = models.Job.objects.first()

        task = base.CWTBaseTask()

        task.on_success(None, 0, None, None)

        mock_get.assert_not_called()

    @mock.patch('wps.tasks.base.models.Job.objects.get')
    def test_cwt_base_task_success(self, mock_get):
        job = models.Job.objects.first()

        mock_get.return_value = job

        task = base.CWTBaseTask()

        task.PUBLISH = base.SUCCESS

        with mock.patch.object(job, 'succeeded') as mock_method:
            task.on_success({'data': 'stuff'}, 0, None, {'job_id': job.id})

            mock_method.assert_called()

        mock_get.assert_called()

    @mock.patch('wps.tasks.base.models.Job.objects.get')
    def test_cwt_base_task_failure_cant_publish(self, mock_get):
        job = models.Job.objects.first()

        task = base.CWTBaseTask()

        task.on_failure(None, 0, None, None, None)

        mock_get.assert_not_called()

    @mock.patch('wps.tasks.base.models.Job.objects.get')
    def test_cwt_base_task_failure(self, mock_get):
        job = models.Job.objects.first()

        mock_get.return_value = job

        task = base.CWTBaseTask()

        task.PUBLISH = base.FAILURE

        with mock.patch.object(job, 'failed') as mock_method:
            task.on_failure(None, 0, None, {'job_id': job.id}, None)

            mock_method.assert_called()

        mock_get.assert_called()

    @mock.patch('wps.tasks.base.models.Job.objects.get')
    def test_cwt_base_task_retry_cant_publish(self, mock_get):
        job = models.Job.objects.first()

        task = base.CWTBaseTask()

        task.on_retry(None, 0, None, None, None)

        mock_get.assert_not_called()

    @mock.patch('wps.tasks.base.models.Job.objects.get')
    def test_cwt_base_task_retry(self, mock_get):
        job = models.Job.objects.first()

        mock_get.return_value = job

        task = base.CWTBaseTask()

        task.PUBLISH = base.RETRY

        with mock.patch.object(job, 'retry') as mock_method:
            task.on_retry(None, 0, None, {'job_id': job.id}, None)

            mock_method.assert_called()

        mock_get.assert_called()

    def test_get_process_missing(self):
        with self.assertRaises(WPSError):
            proc = base.get_process('i do not exist')

    def test_get_process(self):
        @base.register_process('CDAT.test')
        def test_task(self):
            pass

        proc = base.get_process('CDAT.test')

        self.assertEqual(proc, test_task)

    def test_register_process_abstract(self):
        abstract_text = 'CDAT.test abstract text'

        @base.register_process('CDAT.test', abstract=abstract_text)
        def test_task(self):
            pass

        self.assertEqual(test_task.IDENTIFIER, 'CDAT.test')
        self.assertEqual(test_task.ABSTRACT, abstract_text)
        self.assertIn('CDAT.test', base.REGISTRY)

    def test_register_process(self):
        @base.register_process('CDAT.test')
        def test_task(self):
            pass

        self.assertEqual(test_task.IDENTIFIER, 'CDAT.test')
        self.assertEqual(test_task.ABSTRACT, '')
        self.assertIn('CDAT.test', base.REGISTRY)
