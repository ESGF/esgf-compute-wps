import os
import shutil

import cwt
import mock
from django import test

from . import helpers
from wps import models
from wps import settings
from wps.tasks import cdat
from wps.tasks import edas

class EDASTaskTestCase(test.TestCase):
    fixtures = ['processes.json', 'servers.json', 'users.json']

    def setUp(self):
        settings.EDAS_TIMEOUT = 5

        settings.EDAS_HOST = 'unknown'

        self.user = models.User.objects.all()[0]

        self.server = models.Server.objects.all()[0]

        self.process = models.Process.objects.all()[0]

    def test_listen_edas_output(self):
        job = models.Job.objects.create(server=self.server, user=self.user, process=self.process)

        job.accepted()

        job.started()

        expected_path = '/data/temp.nc'

        mock_socket = mock.Mock()

        mock_socket.recv.return_value = 'file!{}'.format(expected_path)

        poller = mock.Mock()

        poller.poll.return_value = [tuple([mock_socket, 1])]

        edas_output_path = edas.listen_edas_output(poller, job)

        self.assertEqual(edas_output_path, expected_path)

    def test_check_exceptions(self):
        data = "!<response><exceptions><exception>error</exception></exceptions></response>"

        with self.assertRaises(Exception):
            edas.check_exceptions(data)

    def test_edas_submit(self):
        job = models.Job.objects.create(server=self.server, user=self.user, process=self.process)

        params = {
            'user_id': self.user.id,
            'job_id': job,
        }

        operation = cwt.Process(identifier='CDSpark.max')

        input_path = os.path.join(os.getcwd(), 'test1.nc')

        variables = [
            cwt.Variable(input_path, 'tas')
        ]

        data_inputs = cwt.WPS('').prepare_data_inputs(operation, variables, None)

        with self.assertNumQueries(1), self.assertRaises(Exception):
            edas.edas_submit(data_inputs, 'CDSpark.max', **params)

class CDATTaskTestCase(test.TestCase):
    fixtures = ['processes.json', 'servers.json', 'users.json']

    @classmethod
    def setUpClass(cls):
        super(CDATTaskTestCase, cls).setUpClass()

        os.makedirs(settings.CACHE_PATH)

        os.makedirs(settings.LOCAL_OUTPUT_PATH)

        time = helpers.generate_time('months since 1990-1-1', 24)

        helpers.write_file('./test1.nc', (time, helpers.latitude, helpers.longitude), 'tas')

    @classmethod
    def tearDownClass(cls):
        super(CDATTaskTestCase, cls).tearDownClass()

        shutil.rmtree(settings.CACHE_PATH)

        shutil.rmtree(settings.LOCAL_OUTPUT_PATH)

        if os.path.exists('./test1.nc'):
            os.remove('./test1.nc')

    def setUp(self):
        settings.DAP = True

        self.server = models.Server.objects.all()[0]

        self.user = models.User.objects.all()[0]

        self.process = models.Process.objects.all()[0]

    @mock.patch('wps.tasks.process.CWTBaseTask.initialize')
    def test_cache_variable(self, mock_initialize):
        job = models.Job.objects.create(server=self.server, user=self.user, process=self.process)

        job.accepted()

        mock_initialize.return_value = (self.user, job) 

        params = {
            'user_id': self.user.id,
            'job_id': job.id,
        }

        variables = {
            'tas': {
                'id': 'tas|tas',
                'uri': os.path.abspath('./test1.nc'),
            }
        }

        domains = {}

        operations = {
            'subset': {
                'name': 'CDAT.subset',
                'input': ['tas'],
            }
        }

        data_inputs = cdat.cache_variable('CDAT.subset', variables, domains, operations, **params)

        self.assertRegexpMatches(data_inputs, '.*http://thredds:8080/threddsCWT/dodsC/public/.*\.nc.*')
