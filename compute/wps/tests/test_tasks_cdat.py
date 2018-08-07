#! /usr/bin/env python

import os
import json

import cwt
import mock
from django import test

from wps.tasks import cdat

class CDATTaskTestCase(test.TestCase):

    @mock.patch('uuid.uuid4')
    @mock.patch('wps.tasks.cdat.read_data')
    @mock.patch('wps.tasks.cdat.write_data')
    def test_base_retrieve_gridder(self, mock_write, mock_read, mock_uuid):
        mock_uuid.return_value = 'test'

        op = cwt.Process('CDAT.subset')

        op.parameters['gridder'] = cwt.Gridder('gaussian~32')

        key = 'file01'

        attrs = {
            key: {
                'path': './file01.nc',
            }
        }

        output_path = '/data/public/test.nc'

        mock_self = mock.MagicMock()
        mock_self.generate_grid.return_value = None

        result = cdat.base_retrieve(mock_self, attrs, [key], op, 'tas', 
                                    'days since 1990-01-01', output_path,
                                    job_id=0)

        mock_self.load_job.assert_called()

        self.assertEqual(mock_self.open.call_args_list[0],
                         mock.call(output_path, 'w'))

        self.assertEqual(mock_self.open.call_args_list[1],
                         mock.call('./file01.nc'))

        self.assertEqual(mock_read.call_args_list[0][0][1:], ('tas',))

        self.assertEqual(mock_write.call_args_list[0][0][:-1], 
                         (mock_read.return_value, 'tas', 
                          'days since 1990-01-01', 
                          mock_self.generate_grid.return_value, 
                          op.parameters['gridder'],))

    @mock.patch('uuid.uuid4')
    @mock.patch('wps.tasks.cdat.read_data')
    @mock.patch('wps.tasks.cdat.write_data')
    def test_base_retrieve_multiple(self, mock_write, mock_read, mock_uuid):
        mock_uuid.return_value = 'test'

        op = cwt.Process('CDAT.subset')

        key = 'file01'
        key2 = 'file02'

        attrs = [
            {
                key: {
                    'path': './file01.nc',
                }
            },
            {
                key2: {
                    'path': './file02.nc',
                }
            }
        ]

        output_path = '/data/public/test.nc'

        mock_self = mock.MagicMock()
        mock_self.generate_grid.return_value = None

        result = cdat.base_retrieve(mock_self, attrs, [key, key2], op, 'tas', 
                                    'days since 1990-01-01', output_path,
                                    job_id=0)

        mock_self.load_job.assert_called()

        self.assertEqual(mock_self.open.call_args_list[0],
                         mock.call(output_path, 'w'))

        self.assertEqual(mock_self.open.call_args_list[1],
                         mock.call('./file01.nc'))

        self.assertEqual(mock_self.open.call_args_list[2],
                         mock.call('./file02.nc'))


        self.assertEqual(mock_read.call_args_list[0][0][1:], ('tas',))

        self.assertEqual(mock_write.call_args_list[0][0][:-1], 
                         (mock_read.return_value, 'tas', 
                          'days since 1990-01-01', None, None,))

    @mock.patch('uuid.uuid4')
    @mock.patch('wps.tasks.cdat.read_data')
    @mock.patch('wps.tasks.cdat.write_data')
    def test_base_retrieve_cached(self, mock_write, mock_read, mock_uuid):
        mock_uuid.return_value = 'test'

        op = cwt.Process('CDAT.subset')

        key = 'file01'

        mapped = {
            'time': slice(100, 200),
            'lat': slice(0, 90),
            'lon': slice(0, 180),
        }

        chunk_list = [slice(0, 1), slice(1, 2)]

        attrs = {
            key: {
                'cached': True,
                'path': './file01.nc',
                'chunk_axis': 'time',
                'chunk_list': chunk_list,
                'mapped': mapped,
            }
        }

        output_path = '/data/public/test.nc'

        mock_self = mock.MagicMock()
        mock_self.generate_grid.return_value = None

        result = cdat.base_retrieve(mock_self, attrs, [key], op, 'tas', 
                                    'days since 1990-01-01', output_path,
                                    job_id=0)

        mock_self.load_job.assert_called()

        self.assertEqual(mock_self.open.call_args_list[0],
                         mock.call(output_path, 'w'))

        self.assertEqual(mock_self.open.call_args_list[1],
                         mock.call('./file01.nc'))

        mapped.update({'time': slice(0, 1)})

        self.assertEqual(mock_read.call_args_list[0][0][1:], ('tas', mapped))

        mapped.update({'time': slice(1, 2)})

        self.assertEqual(mock_read.call_args_list[1][0][1:], ('tas', mapped))

        self.assertEqual(mock_write.call_args_list[0][0][:-1], 
                         (mock_read.return_value, 'tas', 
                          'days since 1990-01-01', None, None,))

    @mock.patch('uuid.uuid4')
    @mock.patch('wps.tasks.cdat.read_data')
    @mock.patch('wps.tasks.cdat.write_data')
    def test_base_retrieve(self, mock_write, mock_read, mock_uuid):
        mock_uuid.return_value = 'test'

        op = cwt.Process('CDAT.subset')

        key = 'file01'

        attrs = {
            key: {
                'path': './file01.nc',
            }
        }

        output_path = '/data/public/test.nc'

        mock_self = mock.MagicMock()
        mock_self.generate_grid.return_value = None

        result = cdat.base_retrieve(mock_self, attrs, [key], op, 'tas', 
                                    'days since 1990-01-01', output_path,
                                    job_id=0)

        mock_self.load_job.assert_called()

        self.assertEqual(mock_self.open.call_args_list[0],
                         mock.call(output_path, 'w'))

        self.assertEqual(mock_self.open.call_args_list[1],
                         mock.call('./file01.nc'))

        self.assertEqual(mock_read.call_args_list[0][0][1:], ('tas',))

        self.assertEqual(mock_write.call_args_list[0][0][:-1], 
                         (mock_read.return_value, 'tas', 
                          'days since 1990-01-01', None, None,))

    @mock.patch('wps.tasks.CWTBaseTask.load_job')
    @mock.patch('wps.tasks.CWTBaseTask.load_user')
    def test_health(self, mock_user, mock_job):
        cdat.health(0, 1)

        mock_user.assert_called_with(0)

        mock_job.assert_called_with(1)

        data = json.dumps({
            'jobs_running': 0,
            'active_users': 0,
            'jobs_queued': 0,
        })

        mock_job.return_value.succeeded.assert_called_with(data)

    @mock.patch('os.remove')
    def test_cleanup(self, mock_remove):
        file_paths = ['file1.nc', 'file2.nc', 'file3.nc']

        cdat.cleanup({}, file_paths, job_id=0)

        self.assertEqual(mock_remove.call_count, 3)
