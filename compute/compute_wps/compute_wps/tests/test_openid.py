from builtins import str
import mock
from django import test
from django.conf import settings
from openid.consumer import consumer

from compute_wps.auth import openid


class OpenIDTestCase(test.TestCase):
    fixtures = ['users.json']
    mock_services = [
        mock.Mock(type_uris=['test_uri_1']),
        mock.Mock(type_uris=['test_uri_2']),
        mock.Mock(type_uris=['test_uri_3']),
    ]

    @mock.patch('compute_wps.views.openid.ax.FetchResponse.fromSuccessResponse')
    def test_handle_attribute_exchange_none(self, mock_fetch):
        mock_fetch.return_value = None

        attrs = openid.handle_attribute_exchange(mock.Mock())

        self.assertEqual(attrs, {})

    @mock.patch('compute_wps.views.openid.ax.FetchResponse.fromSuccessResponse')
    def test_handle_attribute_exchange_exception(self, mock_fetch):
        mock_fetch.return_value = mock.Mock(**{'get.side_effect': KeyError})

        with self.assertRaises(openid.MissingAttributeError) as e:
            openid.handle_attribute_exchange(mock.Mock())

            self.assertEqual(str(e.exception),
                             str(openid.MissingAttributeError('email')))

    @mock.patch('compute_wps.views.openid.ax.FetchResponse.fromSuccessResponse')
    def test_handle_attribute_exchange(self, mock_fetch):
        mock_fetch.return_value = mock.Mock(**{'get.return_value': ['test@test.com']})

        attrs = openid.handle_attribute_exchange(mock.Mock())

        self.assertEqual(attrs['email'], 'test@test.com')

    def test_complete_exception(self):
        with self.assertRaises(Exception):
            openid.complete(mock.Mock())

    @mock.patch('compute_wps.views.openid.consumer.Consumer')
    def test_complete_failure(self, mock_consumer):
        mock_consumer.return_value = mock.Mock(**{'complete.return_value': mock.Mock(status='failure')})

        with self.assertRaises(Exception):
            openid.complete(mock.Mock())

    @mock.patch('compute_wps.views.openid.consumer.Consumer')
    def test_complete_cancel(self, mock_consumer):
        mock_consumer.return_value = mock.Mock(**{'complete.return_value': mock.Mock(status='cancel')})

        with self.assertRaises(Exception):
            openid.complete(mock.Mock())

    @mock.patch('compute_wps.views.openid.handle_attribute_exchange')
    @mock.patch('compute_wps.views.openid.consumer.Consumer')
    def test_complete(self, mock_consumer, mock_attr):
        mock_attr.return_value = {'email': 'test@test.com'}

        mock_complete = mock.Mock(**{'getDisplayIdentifier.return_value': 'http://test.com/openid/test'})

        mock_consumer.return_value = mock.Mock(**{'complete.return_value': mock_complete})

        url, attrs = openid.complete(mock.Mock())

        self.assertEqual(url, 'http://test.com/openid/test')
        self.assertEqual(attrs, mock_attr.return_value)

    @mock.patch('compute_wps.views.openid.manager.Discovery')
    @mock.patch('compute_wps.views.openid.consumer.Consumer')
    def test_begin_exception(self, mock_consumer, mock_discovery):
        mock_consumer.return_value.beginWithoutDiscovery.side_effect = consumer.DiscoveryFailure('error', 404)

        with self.assertRaises(openid.DiscoverError):
            openid.begin(mock.Mock(session={}), 'http://testbad.com/openid',
                         'http://test.com/next')

    @mock.patch('compute_wps.views.openid.consumer.Consumer')
    def test_begin(self, mock_consumer):
        return_url = 'https://test.com/openid/begin?next=http://test.com/next'

        mock_begin = mock.Mock(**{'redirectURL.return_value': return_url})
        mock_consumer.return_value = mock.Mock(**{'beginWithoutDiscovery.return_value': mock_begin})

        url = openid.begin(mock.Mock(session={}), 'http://test.com/openid',
                           'http://test.com/next')

        self.assertEqual(url, return_url)

        mock_consumer.assert_called_with({}, mock_consumer.call_args[0][1])
        mock_consumer.return_value.beginWithoutDiscovery.assert_called()

        return_to = '{!s}?next=http://test.com/next'.format(settings.OPENID_RETURN_TO_URL)

        mock_begin.redirectURL.assert_called_with(settings.OPENID_TRUST_ROOT_URL, return_to)

    @mock.patch('compute_wps.auth.openid.requests')
    def test_services_discovery_error(self, mock_requests):
        mock_requests.get.side_effect = Exception()

        with self.assertRaises(openid.DiscoverError):
            openid.services('http://testbad.com/openid', ['urn.test1', 'urn.test2'])

    @mock.patch('compute_wps.views.openid.discover.discoverYadis')
    @mock.patch('compute_wps.views.openid.requests')
    def test_services_service_not_supported(self, mock_requests, mock_discover):
        mock_discover.return_value = ('http://test.com/openid', self.mock_services)

        with self.assertRaises(openid.ServiceError) as e:
            openid.services('http://test.com/openid', ['urn.test1', 'urn.test2'])

        self.assertEqual(str(e.exception), str(openid.ServiceError('http://test.com/openid', 'urn.test1')))

    @mock.patch('compute_wps.views.openid.discover.discoverYadis')
    @mock.patch('compute_wps.auth.openid.requests.get')
    def test_services(self, mock_get, mock_discover):
        mock_discover.return_value = ('http://test.com/openid', self.mock_services)

        services = openid.services('http://test.com/openid', ['test_uri_1', 'test_uri_3'])

        self.assertEqual(len(services), 2)

    def test_find_service_by_type_not_found(self):
        service = openid.find_service_by_type(self.mock_services, 'test_uri_10')

        self.assertIsNone(service)

    def test_find_service_by_type(self):
        service = openid.find_service_by_type(self.mock_services, 'test_uri_1')

        self.assertIsNotNone(service)
