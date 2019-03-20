#! /usr/bin/env python 

import datetime
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
    @mock.patch('wps.tasks.credentials.refresh_certificate')
    @mock.patch('wps.tasks.credentials.check_certificate')
    def test_load_certificate(self, mock_check, mock_refresh, mock_os, mock_open):
        mock_os.path.exists.return_value = False

        mock_check.return_value = False

        tasks.load_certificate(self.user)        

        mock_refresh.assert_called_once()

        mock_os.makedirs.assert_called_once()

        mock_os.chdir.assert_called_once()

        mock_open.return_value.__enter__.return_value.write.assert_called()
        open_count = mock_open.return_value.__enter__.return_value.write.call_count

        self.assertEqual(open_count, 6)

    @mock.patch('openid.consumer.discover.discoverYadis')
    def test_refresh_certificate_myproxyclient(self, mock_discover):
        self.user.auth.type = 'myproxyclient'

        self.user.auth.cert = 'some cert'

        with self.assertRaises(tasks.CertificateError):
            cert = tasks.refresh_certificate(self.user)

    @mock.patch('openid.consumer.discover.discoverYadis')
    def test_refresh_certificate_error_loading_extra(self, mock_discover):
        mock_services = mock.MagicMock()

        mock_discover.return_value = ('url', mock_services)

        self.user.auth.extra = 'some invalid content'

        self.user.auth.cert = 'some cert'

        with self.assertRaises(tasks.WPSError):
            cert = tasks.refresh_certificate(self.user)

    @mock.patch('openid.consumer.discover.discoverYadis')
    def test_refresh_certificate_missing_token(self, mock_discover):
        mock_services = mock.MagicMock()

        mock_discover.return_value = ('url', mock_services)

        self.user.auth.extra = '{}'

        self.user.auth.cert = 'some cert'

        with self.assertRaises(tasks.WPSError):
            cert = tasks.refresh_certificate(self.user)

    @mock.patch('wps.tasks.credentials.oauth2.get_certificate')
    @mock.patch('wps.tasks.credentials.openid.find_service_by_type')
    @mock.patch('openid.consumer.discover.discoverYadis')
    def test_refresh_certificate(self, mock_discover, mock_find, mock_get):
        mock_get.return_value = ('cert value', 'key value', 'new token value')

        mock_services = mock.MagicMock()

        mock_discover.return_value = ('url', mock_services)

        extra = '{"token": "token value", "state": "state value"}'

        self.user.auth.extra = extra

        self.user.auth.cert = 'some cert'

        cert = tasks.refresh_certificate(self.user)

        expected_extra = '{"token": "new token value", "state": "state value"}'

        self.assertEqual(cert, 'cert valuekey value')
        self.assertEqual(self.user.auth.cert, 'cert valuekey value')
        self.assertEqual(self.user.auth.extra, expected_extra)

    def test_check_certificate_missing_certificate(self):
        with self.assertRaises(tasks.CertificateError):
            tasks.check_certificate(self.user)

    @mock.patch('wps.tasks.credentials.crypto.load_certificate')
    def test_check_certificate_not_after(self, mock_load):
        self.user.auth.cert = 'some cert'

        before = datetime.datetime.now()

        after = datetime.datetime.now() - datetime.timedelta(days=10)

        mock_load.return_value.get_notBefore.return_value = before.strftime(tasks.CERT_DATE_FMT)

        mock_load.return_value.get_notAfter.return_value = after.strftime(tasks.CERT_DATE_FMT)

        self.assertFalse(tasks.check_certificate(self.user))

    @mock.patch('wps.tasks.credentials.crypto.load_certificate')
    def test_check_certificate_not_before(self, mock_load):
        self.user.auth.cert = 'some cert'

        before = datetime.datetime.now() + datetime.timedelta(days=10)

        after = datetime.datetime.now()

        mock_load.return_value.get_notBefore.return_value = before.strftime(tasks.CERT_DATE_FMT)

        mock_load.return_value.get_notAfter.return_value = after.strftime(tasks.CERT_DATE_FMT)

        self.assertFalse(tasks.check_certificate(self.user))

    @mock.patch('wps.tasks.credentials.crypto.load_certificate')
    def test_check_certificate_error_loading(self, mock_load):
        mock_load.side_effect = Exception('some error')

        self.user.auth.cert = 'some cert'

        before = datetime.datetime.now()

        after = datetime.datetime.now()

        mock_load.return_value.get_notBefore.return_value = before.strftime(tasks.CERT_DATE_FMT)

        mock_load.return_value.get_notAfter.return_value = after.strftime(tasks.CERT_DATE_FMT)

        with self.assertRaises(tasks.CertificateError):
            tasks.check_certificate(self.user)

    @mock.patch('wps.tasks.credentials.crypto.load_certificate')
    def test_check_certificate(self, mock_load):
        self.user.auth.cert = 'some cert'

        before = datetime.datetime.now() - datetime.timedelta(days=10)

        after = datetime.datetime.now() + datetime.timedelta(days=10)

        mock_load.return_value.get_notBefore.return_value = before.strftime(tasks.CERT_DATE_FMT)

        mock_load.return_value.get_notAfter.return_value = after.strftime(tasks.CERT_DATE_FMT)

        self.assertTrue(tasks.check_certificate(self.user))
