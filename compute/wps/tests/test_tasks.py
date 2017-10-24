import os
import shutil

import cwt
import mock

from . import helpers
from .common import CommonTestCase
from wps import models
from wps import settings
from wps.tasks import cdat
from wps.tasks import edas

class EDASTaskTestCase(CommonTestCase):

    def setUp(self):
        settings.EDAS_TIMEOUT = 5

        self.edas_process = models.Process.objects.create(identifier='CDAT.subset', backend='Local') 

        self.edas_job = models.Job.objects.create(server=self.server, user=self.user, process=self.edas_process)

        self.edas_job.accepted()

        self.edas_job.started()

    def test_listen_edas_output(self):
        expected_path = '/data/temp.nc'

        mock_socket = mock.Mock()

        mock_socket.recv.return_value = 'file!{}'.format(expected_path)

        poller = mock.Mock()

        poller.poll.return_value = [tuple([mock_socket, 1])]

        edas_output_path = edas.listen_edas_output(poller, self.edas_job)

        self.assertEqual(edas_output_path, expected_path)

    def test_check_exceptions(self):
        data = "!<response><exceptions><exception>error</exception></exceptions></response>"

        with self.assertRaises(Exception):
            edas.check_exceptions(data)

    def test_edas_submit(self):
        params = {
            'user_id': self.user.id,
            'job_id': self.edas_job.id,
        }

        operation = cwt.Process(identifier='CDSpark.max')

        input_path = os.path.join(os.getcwd(), 'test1.nc')

        variables = [
            cwt.Variable(input_path, 'tas')
        ]

        data_inputs = cwt.WPS('').prepare_data_inputs(operation, variables, None)

        with self.assertNumQueries(8), self.assertRaises(Exception):
            edas.edas_submit(data_inputs, 'CDSpark.max', **params)

class CDATTaskTestCase(CommonTestCase):

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

    def test_cache_variable(self):
        server = models.Server.objects.create(host='default')

        process = models.Process.objects.create(identifier='CDAT.subset', backend='Local') 

        job = models.Job.objects.create(server=server, user=self.user, process=process)

        job.accepted()

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

        self.assertRegexpMatches(data_inputs, 'http://thredds:8080/thredds/dodsC/test/public/.*\.nc')
