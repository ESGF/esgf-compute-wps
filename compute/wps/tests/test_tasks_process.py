#! /usr/bin/env python

import cwt
import mock
from django import test

from wps import models
from wps import WPSError
from wps.tasks import process

class ProcessTestCase(test.TestCase):

    fixtures = ['jobs.json', 'users.json', 'servers.json', 'processes.json']

    def setUp(self):
        self.user = models.User.objects.first()

        self.server = models.Server.objects.get(host='default')

        self.process = models.Process.objects.create(identifier='CDAT.test', server=self.server)

        self.job = models.Job.objects.create(server=self.server, user=self.user, process=self.process)

        self.job.accepted()

        self.proc = process.Process('task_id')

        self.proc.user = self.user

        self.proc.job = self.job

    @mock.patch('wps.tasks.file_manager.DataSetCollection.from_variables')
    def test_generate_chunk_map(self, mock_dsc):
        chunk1 = { 'temporal': slice(100, 150), 'spatial': { 'latitude': (-90, 0), 'longitude': (0, 360) } }
        chunk2 = { 'temporal': slice(150, 200), 'spatial': { 'latitude': (-90, 0), 'longitude': (0, 360) } }
        chunk3 = { 'temporal': slice(200, 250), 'spatial': { 'latitude': (-90, 0), 'longitude': (0, 360) } }
        chunk4 = { 'temporal': slice(250, 300), 'spatial': { 'latitude': (-90, 0), 'longitude': (0, 360) } }
        chunk5 = { 'temporal': slice(300, 350), 'spatial': { 'latitude': (-90, 0), 'longitude': (0, 360) } }

        mock_dsc.return_value.partitions.return_value = [
            (mock.MagicMock(**{ 'url': 'file:///test1.nc'}), chunk1),
            (mock.MagicMock(**{ 'url': 'file:///test1.nc'}), chunk2),
            (mock.MagicMock(**{ 'url': 'file:///test2.nc'}), chunk3),
            (mock.MagicMock(**{ 'url': 'file:///test2.nc'}), chunk4),
            (mock.MagicMock(**{ 'url': 'file:///test3.nc'}), chunk5),
        ]

        mock_dsc.return_value.get_base_units.return_value = 'days since 1990-1-1'

        operation = cwt.Process(identifier='CDAT.subset')

        operation.inputs = [
            cwt.Variable('file:///test1.nc', 'tas'),
            cwt.Variable('file:///test2.nc', 'tas'),
            cwt.Variable('file:///test3.nc', 'tas'),
        ]

        operation.domain = cwt.Domain([
            cwt.Dimension('time', 100, 350),
            cwt.Dimension('latitude', -90, 0),
        ])

        expected = {
            'base_units': 'days since 1990-1-1',
            'file:///test1.nc': [chunk1, chunk2],
            'file:///test2.nc': [chunk3, chunk4],
            'file:///test3.nc': [chunk5,]
        }

        result = self.proc.generate_chunk_map(operation) 

        self.assertDictEqual(result, expected)

    @mock.patch('wps.tasks.process.cdms2.createGaussianGrid')
    def test_generate_grid_value_error(self, mock_gaussian):
        with self.assertRaises(WPSError):
            grid = self.proc.generate_grid(cwt.Gridder(grid='gaussian~32.0'), {}, mock.MagicMock())

    @mock.patch('wps.tasks.process.cdms2.createUniformGrid')
    def test_generate_grid_uniform_value_error(self, mock_uniform):
        with self.assertRaises(WPSError):
            grid = self.proc.generate_grid(cwt.Gridder(grid='uniform~32x64.0'), {}, mock.MagicMock())

    @mock.patch('wps.tasks.process.cdms2.MV2.ones')
    @mock.patch('wps.tasks.process.cdms2.createUniformGrid')
    def test_generate_grid_uniform_full_args(self, mock_uniform, mock_ones):
        mock_ones.return_value = mock.MagicMock()

        chunk = mock.MagicMock()

        chunk.getLatitude.return_value.id = 'lat'
        
        chunk.getLongitude.return_value.id = 'lon'

        grid = self.proc.generate_grid(cwt.Gridder(grid='uniform~-90:45:4x0:72:5'), {'lat': (-45, 45), 'lon': (0, 100)}, chunk)

        mock_uniform.assert_called_with(-88.0, 45, 4.0, 2.5, 72, 5.0)

    @mock.patch('wps.tasks.process.cdms2.MV2.ones')
    @mock.patch('wps.tasks.process.cdms2.createUniformGrid')
    def test_generate_grid_uniform_error_parsing(self, mock_uniform, mock_ones):
        mock_ones.return_value = mock.MagicMock()

        chunk = mock.MagicMock()

        chunk.getLatitude.return_value.id = 'lat'
        
        chunk.getLongitude.return_value.id = 'lon'

        with self.assertRaises(WPSError):
            grid = self.proc.generate_grid(cwt.Gridder(grid='uniform~abdx5'), {'lat': (-45, 45), 'lon': (0, 100)}, chunk)

    @mock.patch('wps.tasks.process.cdms2.MV2.ones')
    @mock.patch('wps.tasks.process.cdms2.createUniformGrid')
    def test_generate_grid_uniform(self, mock_uniform, mock_ones):
        mock_ones.return_value = mock.MagicMock()

        chunk = mock.MagicMock()

        chunk.getLatitude.return_value.id = 'lat'
        
        chunk.getLongitude.return_value.id = 'lon'

        grid = self.proc.generate_grid(cwt.Gridder(grid='uniform~4x5'), {'lat': (-45, 45), 'lon': (0, 100)}, chunk)

        mock_uniform.assert_called_with(-88.0, 45, 4.0, 2.5, 72, 5.0)

    @mock.patch('wps.tasks.process.cdms2.createGaussianGrid')
    def test_generate_grid_parse_error(self, mock_gaussian):
        with self.assertRaises(WPSError):
            grid = self.proc.generate_grid(cwt.Gridder(grid='something invalid'), {}, mock.MagicMock())

    @mock.patch('wps.tasks.process.cdms2.MV2.ones')
    @mock.patch('wps.tasks.process.cdms2.createGaussianGrid')
    def test_generate_grid(self, mock_gaussian, mock_ones):
        mock_ones.return_value = mock.MagicMock()

        chunk = mock.MagicMock()

        chunk.getLatitude.return_value.id = 'lat'
        
        chunk.getLongitude.return_value.id = 'lon'

        grid = self.proc.generate_grid(cwt.Gridder(grid='gaussian~32'), {'lat': (-45, 45), 'lon': (0, 100)}, chunk)

        mock_ones.return_value.setAxisList.assert_called_once()
        mock_ones.return_value.assert_called_with(latitude=(-45, 45), longitude=(0, 100))

        mock_gaussian.assert_called_with(32)

    def test_log(self):
        self.job.started()

        self.proc.job.update_status = mock.MagicMock()

        self.proc.log('some message {}', 'hello', percent=10)

        self.proc.job.update_status.assert_called_once()

    @mock.patch('wps.tasks.process.credentials.load_certificate')
    def test_process_missing_user(self, mock_load):
        self.proc = process.Process('task_id')

        with self.assertRaises(WPSError):
            self.proc.initialize(1003, self.job.id)

    @mock.patch('wps.tasks.process.credentials.load_certificate')
    def test_process_missing_job(self, mock_load):
        self.proc = process.Process('task_id')

        with self.assertRaises(WPSError):
            self.proc.initialize(self.user.id, 1003)

    @mock.patch('wps.tasks.process.credentials.load_certificate')
    def test_process(self, mock_load):
        self.proc = process.Process('task_id')

        self.proc.initialize(self.user.id, self.job.id)

        self.assertEqual(self.proc.user, self.user)
        self.assertEqual(self.proc.job, self.job)
