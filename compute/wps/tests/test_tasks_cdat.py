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

        self.assertRegexpMatches(data_inputs, '.*https://aims2.llnl.gov/threddsCWT/dodsC/public/.*\.nc.*')
