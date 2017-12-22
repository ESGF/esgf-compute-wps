import cwt
import mock
from cdms2 import MV2 as MV
from django import test

from . import helpers
from wps import models
from wps import tasks
from wps.tasks import cdat

class CDATTaskTestCase(test.TestCase):
    fixtures = ['processes.json', 'servers.json', 'users.json']

    def setUp(self):
        self.server = models.Server.objects.get(host='default')

        self.user = models.User.objects.first()

        self.process = models.Process.objects.get(identifier='CDAT.subset')
        
    @mock.patch('wps.tasks.process.CWTBaseTask.retrieve_variable')
    @mock.patch('wps.tasks.process.CWTBaseTask.initialize')
    def test_subset_single_file(self, mock_init, mock_retrieve):
        mock_job = mock.MagicMock()

        mock_init.return_value = (self.user, mock_job)

        mock_retrieve.return_value = '/data/public/new_output.nc'

        variables = {
            'v1': {'id': 'tas|v1', 'uri': 'file:///test_2010-2015.nc'},
        }

        operation = {'name': 'CDAT.aggregate', 'input': ['v1'], 'axes': 'lat|lon'}

        result = cdat.subset({}, variables, {}, operation, user_id=self.user.id, job_id=0)

        self.assertIsInstance(result, dict)

        mock_job.started.assert_called()

        mock_init.assert_called_with(credentials=True, job_id=0, user_id=self.user.id)

        args = mock_retrieve.call_args[0]

        self.assertEqual(args[0][0].uri, 'file:///test_2010-2015.nc')

    @mock.patch('wps.tasks.process.CWTBaseTask.retrieve_variable')
    @mock.patch('wps.tasks.process.CWTBaseTask.initialize')
    def test_subset(self, mock_init, mock_retrieve):
        mock_job = mock.MagicMock()

        mock_init.return_value = (self.user, mock_job)

        mock_retrieve.return_value = '/data/public/new_output.nc'

        variables = {
            'v0': {'id': 'tas|v0', 'uri': 'file:///test_2015-2017.nc'},
            'v1': {'id': 'tas|v1', 'uri': 'file:///test_2010-2015.nc'},
        }

        operation = {'name': 'CDAT.aggregate', 'input': ['v0', 'v1'], 'axes': 'lat|lon'}

        result = cdat.subset({}, variables, {}, operation, user_id=self.user.id, job_id=0)

        self.assertIsInstance(result, dict)

        mock_job.started.assert_called()

        mock_init.assert_called_with(credentials=True, job_id=0, user_id=self.user.id)

        args = mock_retrieve.call_args[0]

        self.assertEqual(args[0][0].uri, 'file:///test_2010-2015.nc')

    @mock.patch('wps.tasks.process.CWTBaseTask.retrieve_variable')
    @mock.patch('wps.tasks.process.CWTBaseTask.initialize')
    def test_aggregate(self, mock_init, mock_retrieve):
        mock_job = mock.MagicMock()

        mock_init.return_value = (self.user, mock_job)

        mock_retrieve.return_value = '/data/public/new_output.nc'

        variables = {
            'v0': {'id': 'tas|v0', 'uri': 'file:///test_2015-2017.nc'},
            'v1': {'id': 'tas|v1', 'uri': 'file:///test_2010-2015.nc'},
        }

        operation = {'name': 'CDAT.aggregate', 'input': ['v0', 'v1'], 'axes': 'lat|lon'}

        result = cdat.aggregate({}, variables, {}, operation, user_id=self.user.id, job_id=0)

        self.assertIsInstance(result, dict)

        mock_job.started.assert_called()

        mock_init.assert_called_with(credentials=True, job_id=0, user_id=self.user.id)

        args = mock_retrieve.call_args[0]

        self.assertEqual(args[0][0].uri, 'file:///test_2010-2015.nc')
        self.assertEqual(args[0][1].uri, 'file:///test_2015-2017.nc')

    @mock.patch('wps.tasks.cdat.base_process')
    def test_average(self, mock_process):
        variables = {
            'v0': {'id': 'tas|v0', 'uri': 'file:///test_2015-2017.nc'},
            'v1': {'id': 'tas|v1', 'uri': 'file:///test_2010-2015.nc'},
        }

        operation = {'name': 'CDAT.aggregate', 'input': ['v0', 'v1'], 'axes': 'lat|lon'}

        cdat.average({}, variables, {}, operation)

        args = mock_process.call_args[0]

        self.assertEqual(args[2], variables)
        self.assertEqual(args[4], operation)
        self.assertEqual(args[5], MV.average)

    @mock.patch('wps.tasks.cdat.base_process')
    def test_maximum(self, mock_process):
        variables = {
            'v0': {'id': 'tas|v0', 'uri': 'file:///test_2015-2017.nc'},
            'v1': {'id': 'tas|v1', 'uri': 'file:///test_2010-2015.nc'},
        }

        operation = {'name': 'CDAT.aggregate', 'input': ['v0', 'v1'], 'axes': 'lat|lon'}

        cdat.maximum({}, variables, {}, operation)

        args = mock_process.call_args[0]

        self.assertEqual(args[2], variables)
        self.assertEqual(args[4], operation)
        self.assertEqual(args[5], MV.max)

    @mock.patch('wps.tasks.cdat.base_process')
    def test_minimum(self, mock_process):
        variables = {
            'v0': {'id': 'tas|v0', 'uri': 'file:///test_2015-2017.nc'},
            'v1': {'id': 'tas|v1', 'uri': 'file:///test_2010-2015.nc'},
        }

        operation = {'name': 'CDAT.aggregate', 'input': ['v0', 'v1'], 'axes': 'lat|lon'}

        cdat.minimum({}, variables, {}, operation)

        args = mock_process.call_args[0]

        self.assertEqual(args[2], variables)
        self.assertEqual(args[4], operation)
        self.assertEqual(args[5], MV.min)

    @mock.patch('wps.tasks.cdat.base_process')
    def test_sum(self, mock_process):
        variables = {
            'v0': {'id': 'tas|v0', 'uri': 'file:///test_2015-2017.nc'},
            'v1': {'id': 'tas|v1', 'uri': 'file:///test_2010-2015.nc'},
        }

        operation = {'name': 'CDAT.aggregate', 'input': ['v0', 'v1'], 'axes': 'lat|lon'}

        cdat.sum({}, variables, {}, operation)

        args = mock_process.call_args[0]

        self.assertEqual(args[2], variables)
        self.assertEqual(args[4], operation)
        self.assertEqual(args[5], MV.sum)

    @mock.patch('wps.tasks.cdat.base_process')
    def test_difference(self, mock_process):
        variables = {
            'v0': {'id': 'tas|v0', 'uri': 'file:///test_2015-2017.nc'},
            'v1': {'id': 'tas|v1', 'uri': 'file:///test_2010-2015.nc'},
        }

        operation = {'name': 'CDAT.aggregate', 'input': ['v0', 'v1'], 'axes': 'lat|lon'}

        cdat.difference({}, variables, {}, operation)

        args = mock_process.call_args[0]

        self.assertEqual(args[2], variables)
        self.assertEqual(args[4], operation)

    @mock.patch('wps.tasks.process.CWTBaseTask.process_variable')
    @mock.patch('wps.tasks.process.CWTBaseTask.initialize')
    def test_base_process_spatial_axes(self, mock_init, mock_process):
        mock_job = mock.MagicMock()

        mock_init.return_value = (self.user, mock_job)

        mock_process.return_value = '/data/public/new_output.nc'

        mv_process = mock.Mock()

        base = tasks.CWTBaseTask()

        variables = {
            'v0': {'id': 'tas|v0', 'uri': 'file:///test_2015-2017.nc'},
            'v1': {'id': 'tas|v1', 'uri': 'file:///test_2010-2015.nc'},
        }

        operation = {'name': 'CDAT.aggregate', 'input': ['v0', 'v1'], 'axes': 'lat|lon'}

        result = cdat.base_process(base, {}, variables, {}, operation, mv_process, job_id=0, user_id=self.user.id)

        self.assertIsInstance(result, dict)

        mock_job.started.assert_called()

        mock_init.assert_called_with(credentials=True, job_id=0, user_id=self.user.id)

        args = mock_process.call_args[1]

        self.assertEqual(args['regridTool'], 'None')
        self.assertEqual(args['regridMethod'], 'None')
        self.assertEqual(args['grid'], None)
        self.assertEqual(args['axes'], ['lat', 'lon'])

    @mock.patch('wps.tasks.process.CWTBaseTask.process_variable')
    @mock.patch('wps.tasks.process.CWTBaseTask.initialize')
    def test_base_process(self, mock_init, mock_process):
        mock_job = mock.MagicMock()

        mock_init.return_value = (self.user, mock_job)

        mock_process.return_value = '/data/public/new_output.nc'

        mv_process = mock.Mock()

        base = tasks.CWTBaseTask()

        variables = {
            'v0': {'id': 'tas|v0', 'uri': 'file:///test_2015-2017.nc'},
            'v1': {'id': 'tas|v1', 'uri': 'file:///test_2010-2015.nc'},
        }

        operation = {'name': 'CDAT.aggregate', 'input': ['v0', 'v1']}

        result = cdat.base_process(base, {}, variables, {}, operation, mv_process, job_id=0, user_id=self.user.id)

        self.assertIsInstance(result, dict)

        mock_job.started.assert_called()

        mock_init.assert_called_with(credentials=True, job_id=0, user_id=self.user.id)

        args = mock_process.call_args[1]

        self.assertEqual(args['regridTool'], 'None')
        self.assertEqual(args['regridMethod'], 'None')
        self.assertEqual(args['grid'], None)
        self.assertEqual(args['axes'], ['time'])

    @mock.patch('wps.tasks.process.CWTBaseTask.retrieve_variable')
    @mock.patch('wps.tasks.process.CWTBaseTask.initialize')
    def test_cache_variable(self, mock_init, mock_retrieve):
        mock_retrieve.return_value = '/data/public/new_output.nc'

        mock_job = mock.MagicMock()

        mock_init.return_value = (self.user, mock_job)

        variables = {
            'v0': {'id': 'tas|v0', 'uri': 'file:///test_2015-2017.nc'},
            'v1': {'id': 'tas|v1', 'uri': 'file:///test_2010-2015.nc'},
        }

        operation = {'name': 'CDAT.aggregate', 'input': ['v0', 'v1']}

        result = cdat.cache_variable({}, variables, {}, operation, job_id=0, user_id=self.user.id)

        self.assertIsInstance(result, dict)

        mock_job.started.assert_called()

        mock_init.assert_called_with(credentials=True, job_id=0, user_id=self.user.id)

        args = mock_retrieve.call_args[0]

        self.assertEqual(args[0][0].uri, 'file:///test_2010-2015.nc')
        self.assertEqual(args[0][1].uri, 'file:///test_2015-2017.nc')
