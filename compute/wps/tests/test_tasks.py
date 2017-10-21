import os
import shutil

import cwt

from . import helpers
from .common import CommonTestCase
from wps import models
from wps import settings
from wps.tasks import cdat
from wps.tasks import edas

#class EDASTaskTestCase(CommonTestCase):
#
#    def test_edas_submit(self):
#        server = models.Server.objects.create(host='default')
#
#        process = models.Process.objects.create(identifier='CDAT.subset', backend='Local') 
#
#        job = models.Job.objects.create(server=server, user=self.user, process=process)
#
#        job.accepted()
#
#        params = {
#            'user_id': self.user.id,
#            'job_id': job.id,
#        }
#
#        operation = cwt.Process(identifier='CDSpark.max')
#
#        variables = [
#            cwt.Variable(os.path.abspath('./test1.nc'), 'tas')
#        ]
#
#        data_inputs = cwt.WPS('').prepare_data_inputs(operation, variables, None)
#
#        edas.edas_submit('', 'CDSpark.max', **params)

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
