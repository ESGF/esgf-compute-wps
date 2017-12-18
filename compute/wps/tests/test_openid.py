import mock
from django import test

from wps import settings
from wps.auth import openid

class OpenIDTestCase(test.TestCase):
    fixtures = ['users.json']
    mock_services = [
        mock.Mock(type_uris=['test_uri_1']),
        mock.Mock(type_uris=['test_uri_2']),
        mock.Mock(type_uris=['test_uri_3']),
    ]

    @mock.patch('wps.views.openid.ax.FetchResponse.fromSuccessResponse')
    def test_handle_openid_attribute_exchange_none(self, mock_fetch):
        mock_fetch.return_value = None

        attrs = openid.handle_openid_attribute_exchange(mock.Mock())

        self.assertEqual(attrs, {'email': None})

    @mock.patch('wps.views.openid.ax.FetchResponse.fromSuccessResponse')
    def test_handle_openid_attribute_exchange(self, mock_fetch):
        mock_fetch.return_value = mock.Mock(**{'get.return_value': ['test@test.com']})

        attrs = openid.handle_openid_attribute_exchange(mock.Mock())

        self.assertEqual(attrs['email'], 'test@test.com')

    def test_openid_complete_exception(self):
        with self.assertRaises(Exception):
            openid.complete(mock.Mock())

    @mock.patch('wps.views.openid.consumer.Consumer')
    def test_openid_complete_failure(self, mock_consumer):
        mock_consumer.return_value = mock.Mock(**{'complete.return_value': mock.Mock(status='failure')})

        with self.assertRaises(Exception):
            openid.complete(mock.Mock())

    @mock.patch('wps.views.openid.consumer.Consumer')
    def test_openid_complete_cancel(self, mock_consumer):
        mock_consumer.return_value = mock.Mock(**{'complete.return_value': mock.Mock(status='cancel')})

        with self.assertRaises(Exception):
            openid.complete(mock.Mock())

    @mock.patch('wps.views.openid.handle_openid_attribute_exchange')
    @mock.patch('wps.views.openid.consumer.Consumer')
    def test_openid_complete(self, mock_consumer, mock_attr):
        mock_attr.return_value = {'email': 'test@test.com'}

        mock_complete = mock.Mock(**{'getDisplayIdentifier.return_value': 'http://test.com/openid/test'})

        mock_consumer.return_value = mock.Mock(**{'complete.return_value': mock_complete})

        url, attrs = openid.complete(mock.Mock())

        self.assertEqual(url, 'http://test.com/openid/test')
        self.assertEqual(attrs, mock_attr.return_value)

    def test_openid_begin_exception(self):
        with self.assertRaises(Exception):
            openid.openid_begin(mock.Mock(), 'http://test.com/openid')

    @mock.patch('wps.views.openid.consumer.Consumer')
    def test_openid_begin(self, mock_consumer):
        mock_begin = mock.Mock(**{'redirectURL.return_value': 'http://test.com/openid/begin'})
        mock_consumer.return_value = mock.Mock(**{'begin.return_value': mock_begin})

        url = openid.begin(mock.Mock(session={}), 'http://test.com/openid')

        self.assertEqual(url, 'http://test.com/openid/begin')

        mock_consumer.assert_called_with({}, mock_consumer.call_args[0][1])
        mock_consumer.return_value.begin.assert_called_with('http://test.com/openid')

        mock_begin.redirectURL.assert_called_with(settings.OPENID_TRUST_ROOT, settings.OPENID_RETURN_TO)

    def test_openid_services_discovery_error(self):
        with self.assertRaises(Exception):
            openid.services('http://test.com/openid', ['urn.test1', 'urn.test2'])

    @mock.patch('wps.views.openid.discover.discoverYadis')
    def test_openid_services_service_not_supported(self, mock_discover):
        mock_discover.return_value = ('http://test.com/openid', self.mock_services)

        with self.assertRaises(Exception):
            openid.services('http://test.com/openid', ['urn.test1', 'urn.test2'])

    @mock.patch('wps.views.openid.discover.discoverYadis')
    def test_openid_services(self, mock_discover):
        mock_discover.return_value = ('http://test.com/openid', self.mock_services)

        services = openid.services('http://test.com/openid', ['test_uri_1', 'test_uri_3'])

        self.assertEqual(len(services), 2)

    def test_openid_find_service_by_type_not_found(self):
        service = openid.find_service_by_type(self.mock_services, 'test_uri_10')

        self.assertIsNone(service)

    def test_openid_find_service_by_type(self):
        service = openid.find_service_by_type(self.mock_services, 'test_uri_1')

        self.assertIsNotNone(service)

