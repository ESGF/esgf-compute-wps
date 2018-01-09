#! /usr/bin/env python 

import mock

from django import test

from wps import models
from wps import tasks
from wps import WPSError

class CredentailsTestCase(test.TestCase):

    fixtures = ['users.json']

    def setUp(self):
        self.user = models.User.objects.first()

    @mock.patch('wps.tasks.credentials.open')
    @mock.patch('wps.tasks.credentials.os')
    def test_cwt_task_load_certificate_refresh_certificate(self, mock_os, mock_open):
        tasks.check_certificate = mock.Mock(return_value=False)

        tasks.refresh_certificate = mock.Mock()
        
        tasks.load_certificate(self.user)

        tasks.refresh_certificate.assert_called()

    @mock.patch('wps.tasks.credentials.open')
    @mock.patch('wps.tasks.credentials.os')
    def test_cwt_task_load_certificate_user_path_does_not_exist(self, mock_os, mock_open):
        mock_os.path.exists.return_value = False

        tasks.check_certificate = mock.Mock(return_value=True)
        
        tasks.load_certificate(self.user)

        mock_os.makedirs.assert_called()

    @mock.patch('wps.tasks.credentials.open')
    @mock.patch('wps.tasks.credentials.os')
    def test_cwt_task_load_certificate(self, mock_os, mock_open):
        tasks.check_certificate = mock.Mock(return_value=True)
        
        tasks.load_certificate(self.user)

    def test_cwt_task_refresh_certificate_myproxyclient(self):
        self.user.auth.type = 'myproxyclient'

        self.user.auth.save()

        with self.assertRaises(tasks.CertificateError):
            tasks.refresh_certificate(self.user)

    @mock.patch('wps.tasks.credentials.discover')
    def test_cwt_task_refresh_certificate_discovery_error(self, mock_discover):
        mock_discover.discoverYadis.side_effect = discover.DiscoveryFailure('url', 401)

        with self.assertRaises(discover.DiscoveryFailure) as e:
            tasks.refresh_certificate(self.user)

    @mock.patch('wps.tasks.credentials.discover')
    def test_cwt_task_refresh_certificate_missing_oauth2_state(self, mock_discover):
        mock_discover.discoverYadis.return_value = ('url', {})

        with self.assertRaises(WPSError) as e:
            tasks.refresh_certificate(self.user)

    @mock.patch('wps.tasks.credentials.discover')
    def test_cwt_task_refresh_certificate_missing_oauth2_token(self, mock_discover):
        self.user.auth.extra = '{}'

        self.user.auth.save()

        mock_discover.discoverYadis.return_value = ('url', {})

        with self.assertRaises(WPSError) as e:
            tasks.refresh_certificate(self.user)

    @mock.patch('wps.tasks.credentials.openid')
    @mock.patch('wps.tasks.credentials.oauth2')
    @mock.patch('wps.tasks.credentials.discover')
    def test_cwt_task_refresh_certificate(self, mock_discover, mock_oauth2, mock_openid):
        mock_openid.find_service_by_type.side_effect = [mock.MagicMock(), mock.MagicMock()]

        mock_oauth2.get_certificate.return_value = ('cert', 'key', 'new_token')

        self.user.auth.extra = '{"token": "oauth2_token"}'

        self.user.auth.save()

        mock_discover.discoverYadis.return_value = ('url', {})

        with self.assertNumQueries(1):
            data = tasks.refresh_certificate(self.user)

        self.assertEqual(data, 'certkey')

    def test_cwt_task_check_certificate_missing(self):
        with self.assertRaises(tasks.CertificateError) as e:
            tasks.check_certificate(self.user)

    @mock.patch('wps.tasks.credentials.crypto')
    def test_cwt_task_check_certificate_not_before(self, mock_crypto):
        expired = datetime.datetime.now() + datetime.timedelta(days=10)

        mock_crypto.load_certificate.return_value.get_notBefore.return_value = expired.strftime(tasks.CERT_DATE_FMT)
        
        mock_crypto.load_certificate.return_value.get_notAfter.return_value = datetime.datetime.now().strftime(tasks.CERT_DATE_FMT)

        self.user.auth.cert = 'some certificate'

        self.user.auth.save()

        result = tasks.check_certificate(self.user)

        self.assertFalse(result)

    @mock.patch('wps.tasks.credentials.crypto')
    def test_cwt_task_check_certificate_not_after(self, mock_crypto):
        expired = datetime.datetime.now() - datetime.timedelta(days=10)

        mock_crypto.load_certificate.return_value.get_notBefore.return_value = datetime.datetime.now().strftime(tasks.CERT_DATE_FMT)
        
        mock_crypto.load_certificate.return_value.get_notAfter.return_value = expired.strftime(tasks.CERT_DATE_FMT)

        self.user.auth.cert = 'some certificate'

        self.user.auth.save()

        result = tasks.check_certificate(self.user)

        self.assertFalse(result)

    @mock.patch('wps.tasks.credentials.crypto')
    def test_cwt_task_check_certificate_fail_to_load(self, mock_crypto):
        mock_crypto.load_certificate.side_effect = Exception('Failed to load certificate')

        self.user.auth.cert = 'some certificate'

        self.user.auth.save()

        with self.assertRaises(tasks.CertificateError) as e:
            result = tasks.check_certificate(self.user)

    @mock.patch('wps.tasks.credentials.crypto')
    def test_cwt_task_check_certificate(self, mock_crypto):
        not_before = datetime.datetime.now() - datetime.timedelta(days=10)

        not_after = datetime.datetime.now() + datetime.timedelta(days=10)

        mock_crypto.load_certificate.return_value.get_notBefore.return_value = not_before.strftime(tasks.CERT_DATE_FMT)
        
        mock_crypto.load_certificate.return_value.get_notAfter.return_value = not_after.strftime(tasks.CERT_DATE_FMT)

        self.user.auth.cert = 'some certificate'

        self.user.auth.save()

        result = tasks.check_certificate(self.user)

        self.assertTrue(result)
