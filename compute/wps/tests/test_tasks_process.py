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

    def test_generate_chunk_map(self):
        dataset1 = mock.MagicMock()
        dataset1.url = 'file:///test1.nc'
        dataset1.variable_name = 'tas'
        dataset1.temporal = slice(0, 10)
        dataset1.spatial = { 'lat': (-90, 0), 'lon': (0, 360) }

        dataset2 = mock.MagicMock()
        dataset2.url = 'file:///test2.nc'
        dataset2.variable_name = 'tas'
        dataset2.temporal = slice(10, 20)
        dataset2.spatial = { 'lat': (-90, 0), 'lon': (0, 360) }

        dataset3 = mock.MagicMock()
        dataset3.url = 'file:///test3.nc'
        dataset3.variable_name = 'tas'
        dataset3.temporal = slice(20, 30)
        dataset3.spatial = { 'lat': (-90, 0), 'lon': (0, 360) }

        collection = mock.MagicMock()

        collection.partitions.return_value = [
            (dataset1, 'test1'),
            (dataset1, 'test1.1'),
            (dataset2, 'test2'),
            (dataset3, 'test3'),
        ]

        collection.get_base_units.return_value = 'days since 1990-1-1'

        domain = cwt.Domain([
            cwt.Dimension('time', 100, 400),
            cwt.Dimension('lat', -90, 0),
        ])

        chunk_map = self.proc.generate_chunk_map(collection, domain)

        self.assertEqual(len(chunk_map), 3)
        self.assertItemsEqual(chunk_map.keys(), ['file:///test1.nc', 'file:///test2.nc', 'file:///test3.nc'])

        expected = {
            'variable_name': 'tas',
            'temporal': slice(0, 10),
            'spatial': { 'lat': (-90, 0), 'lon': (0, 360) },
            'base_units': 'days since 1990-1-1',
            'chunks': ['test1', 'test1.1'],
        }

        self.assertEqual(chunk_map['file:///test1.nc'], expected)

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
