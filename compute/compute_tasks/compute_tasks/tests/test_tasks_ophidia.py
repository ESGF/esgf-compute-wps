import unittest

import mock

from wps import models
from wps import WPSError
from wps.tasks import ophidia

#class OphidiaTasksTestCase(test.TestCase):
#    fixtures = ['users.json']
#
#    def setUp(self):
#        self.user = models.User.objects.first()
#
#    def test_workflow_fail_last_error_parse(self):
#        response = {
#            'response': []
#        }
#
#        mock_client = mock.MagicMock(**{'deserialize_response.return_value': response})
#
#        mock_client.last_error = 'some error'
#
#        workflow = ophidia.OphidiaWorkflow(mock_client)
#
#        with self.assertRaises(WPSError) as e:
#            workflow.submit()        
#
#            self.assertIn('error1: description1', str(e))
#            self.assertIn('error2: description2', str(e))
#
#    def test_workflow_check_error(self):
#        response = {
#            'response': [
#                {},
#                {},
#                {
#                    'objcontent': [
#                        {
#                            'rowvalues': [
#                                ['error1', '', 'description1'],
#                                ['error2', '', 'description2'],
#                            ]
#                        }
#                    ]
#                },
#            ]
#        }
#
#        mock_client = mock.MagicMock(**{'deserialize_response.return_value': response})
#
#        mock_client.last_error = 'some error'
#
#        workflow = ophidia.OphidiaWorkflow(mock_client)
#
#        with self.assertRaises(WPSError) as e:
#            workflow.submit()        
#
#            self.assertIn('error1: description1', str(e))
#            self.assertIn('error2: description2', str(e))
#
#    def test_workflow(self):
#        mock_client = mock.MagicMock()
#
#        mock_client.last_error = None
#
#        workflow = ophidia.OphidiaWorkflow(mock_client)
#
#        workflow.submit()        
#
#        mock_client.wsubmit.assert_called()
#
#    @mock.patch('wps.tasks.process.Process')
#    @mock.patch('wps.tasks.ophidia.client.Client')
#    def test_oph_submit_define_axes(self, mock_client, mock_process):
#        mock_client.return_value.last_error = None
#
#        variables = {
#            'v0': {'id': 'tas|v0', 'uri': 'file:///test.nc'},
#        }
#
#        operation = {'name': 'Oph.max', 'input': ['v0'], 'axes': 'lat|lon'}
#
#        result = ophidia.oph_submit({}, variables, {}, operation, user_id=self.user.id, job_id=0)
#
#        self.assertIsInstance(result, dict)
#        
#    @mock.patch('wps.tasks.process.Process')
#    @mock.patch('wps.tasks.ophidia.client.Client')
#    def test_oph_submit_define_cores(self, mock_client, mock_process):
#        mock_client.return_value.last_error = None
#
#        variables = {
#            'v0': {'id': 'tas|v0', 'uri': 'file:///test.nc'},
#        }
#
#        operation = {'name': 'Oph.max', 'input': ['v0'], 'cores': '10'}
#
#        result = ophidia.oph_submit({}, variables, {}, operation, user_id=self.user.id, job_id=0)
#
#        self.assertIsInstance(result, dict)
#
#    @mock.patch('wps.tasks.process.Process')
#    @mock.patch('wps.tasks.ophidia.client.Client')
#    def test_oph_submit(self, mock_client, mock_process):
#        mock_client.return_value.last_error = None
#
#        variables = {
#            'v0': {'id': 'tas|v0', 'uri': 'file:///test.nc'},
#        }
#
#        operation = {'name': 'Oph.max', 'input': ['v0']}
#
#        result = ophidia.oph_submit({}, variables, {}, operation, user_id=self.user.id, job_id=0)
#
#        self.assertIsInstance(result, dict)
