import cwt
import mock
from django import test
from django.conf import settings

from wps import models
from wps import WPSError
from wps.backends import ophidia

class OphidiaBackendTestCase(test.TestCase):
    fixtures = ['servers.json', 'users.json', 'processes.json']

    def setUp(self):
        self.backend = ophidia.Ophidia()

        self.user = models.User.objects.first()

    def test_execute_missing_operation(self):
        mock_job = mock.MagicMock()

        variables = {
            'v0': cwt.Variable('file:///test.nc', 'tas', name='v0'),
        }

        domains = {'d0': cwt.Domain([cwt.Dimension('time', 0, 200)])}

        with self.assertRaises(WPSError) as e:
            self.backend.execute('Oph.max', variables, domains, {}, job=mock_job, user=self.user)

    def test_execute(self):
        mock_job = mock.MagicMock()

        variables = {
            'v0': cwt.Variable('file:///test.nc', 'tas', name='v0'),
        }

        domains = {'d0': cwt.Domain([cwt.Dimension('time', 0, 200)])}

        operation = cwt.Process(identifier='Oph.max', name='max')

        operation.domain = 'd0'

        result = self.backend.execute('Oph.max', variables, domains, {'max': operation}, job=mock_job, user=self.user)

        self.assertIsNotNone(result)

    def test_populate_processes(self):
        process_count = len(ophidia.PROCESSES)

        self.backend.populate_processes()

        self.assertEqual(len(self.backend.processes), process_count)

    def test_initialize(self):
        with self.assertNumQueries(0):
            self.backend.initialize()
