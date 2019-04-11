import json

import mock
from django import test

from wps.views import esgf


class ESGFViewsTestCase(test.TestCase):
    fixtures = ['users.json']

    def setUp(self):
        self.user = mock.MagicMock()

        self.time = mock.MagicMock()
        type(self.time).id = mock.PropertyMock(return_value='time')
        self.time.__getitem__.side_effect = [1000, 2000]
        type(self.time).units = mock.PropertyMock(return_value='days since 1990')
        self.time.__len__.return_value = 200
        self.time.isTime.return_value = True

        type(self.time.clone.return_value).id = mock.PropertyMock(return_value='time')
        self.time.clone.return_value.__getitem__.side_effect = [1000, 2000]
        type(self.time.clone.return_value).units = mock.PropertyMock(return_value='days since 1990')
        self.time.clone.return_value.__len__.return_value = 200
        self.time.asComponentTime.return_value.__getitem__.side_effect = [
            '2018-12-05',
            '2020-12-05',
        ]
        self.time.clone.return_value.asComponentTime.return_value.__getitem__.side_effect = [
            '2018-12-05',
            '2020-12-05',
        ]

        self.lat = mock.MagicMock()
        type(self.lat).id = mock.PropertyMock(return_value='lat')
        self.lat.__getitem__.side_effect = [100, 200]
        type(self.lat).units = mock.PropertyMock(return_value='degress north')
        self.lat.__len__.return_value = 100
        self.lat.isTime.return_value = False

        self.time_desc = {
            'id': 'time',
            'start': 1000.0,
            'stop': 2000.0,
            'units': 'days since 1990',
            'start_timestamp': '2018-12-05',
            'stop_timestamp': '2020-12-05',
            'length': 200,
        }

        self.lat_desc = {
            'id': 'lat',
            'start': 100.0,
            'stop': 200.0,
            'units': 'degress north',
            'length': 100,
        }

        self.process_url_output = {
            'url': 'file:///test1.nc',
            'temporal': self.time_desc,
            'spatial': [
                self.lat_desc,
            ],
        }

        self.docs = {
            'response': {
                'docs': [
                    {
                        'variable': ['tas'],
                        'url': [
                            'file:///test1.nc.html|application/dap|opendap',
                        ]
                    }
                ],
            }
        }

        self.docs_parsed = {
            'files': [
                'file:///test1.nc'
            ],
            'variables': {
                'tas': [0]
            }
        }

    @mock.patch('requests.get')
    @mock.patch('wps.views.esgf.cache')
    def test_search_solr_cached(self, mock_cache, mock_get):
        mock_cache.get.return_value = self.docs_parsed

        type(mock_get.return_value).content = mock.PropertyMock(
            return_value=json.dumps(self.docs))

        esgf.search_solr('dataset_id', 'index_node')

        mock_cache.set.assert_not_called()

    @mock.patch('requests.get')
    @mock.patch('wps.views.esgf.cache')
    def test_search_solr(self, mock_cache, mock_get):
        mock_cache.get.return_value = None

        type(mock_get.return_value).content = mock.PropertyMock(
            return_value=json.dumps(self.docs))

        esgf.search_solr('dataset_id', 'index_node')

        mock_cache.set.assert_called_with('dataset_id', self.docs_parsed, 24*60*60)

    def test_parse_solr_docs(self):
        data = esgf.parse_solr_docs(self.docs)

        self.assertEqual(data, self.docs_parsed)

    def test_search_params(self):
        data = esgf.search_params('dataset_id', 'tas', None)

        expected = {
            'type': 'File',
            'dataset_id': 'dataset_id',
            'format': 'application/solr+json',
            'offset': 0,
            'limit': 10000,
            'distrib': 'true',
            'query': 'tas',
        }

        self.assertEqual(data, expected)

    @mock.patch('wps.views.esgf.process_url')
    def test_retrieve_axes(self, mock_process):
        mock_process.return_value = self.process_url_output

        data = esgf.retrieve_axes(self.user, 'dataset_id', 'tas',
                                  ['file:///test1.nc'])

        self.assertEqual(mock_process.call_args[0][1:2], ('dataset_id|tas',))

        self.assertEqual(data, [self.process_url_output])

    @mock.patch('wps.views.esgf.process_axes')
    @mock.patch('cdms2.open')
    @mock.patch('wps.views.esgf.cache')
    def test_process_url_cached(self, mock_cache, mock_open, mock_process):
        mock_cache.get.return_value = {}

        mock_process.side_effect = [
            self.process_url_output
        ]

        user = mock.MagicMock()

        context = mock.MagicMock()

        data = esgf.process_url(user, 'prefix', context)

        self.assertEqual(data, {})

        mock_process.assert_not_called()

    @mock.patch('wps.views.esgf.process_axes')
    @mock.patch('cdms2.open')
    @mock.patch('wps.views.esgf.cache')
    @mock.patch('hashlib.md5')
    @mock.patch('wps.views.esgf.OperationContext')
    def test_process_url(self, mock_ctx, mock_md5, mock_cache, mock_open, mock_process):
        mock_md5.return_value.hexdigest.return_value = 'id'

        mock_cache.get.return_value = None

        mock_process.side_effect = [
            self.process_url_output
        ]

        user = mock.MagicMock()

        context = mock.MagicMock()

        type(context).variable = mock.MagicMock(**{'uri':
                                                   'file:///test1.nc'})

        data = esgf.process_url(user, 'prefix', context)

        self.assertEqual(data, self.process_url_output)

        mock_process.assert_called()

        mock_cache.set.assert_called_with('id', self.process_url_output,
                                          24*60*60)

    def test_process_axes(self):
        self.maxDiff = None
        header = mock.MagicMock()

        header.getAxisList.return_value = [self.time, self.lat]

        data = esgf.process_axes(header)

        expected = {
            'temporal': self.time_desc,
            'spatial': [self.lat_desc],
        }

        self.assertEqual(data, expected)

    def test_describe_axis(self):
        data = esgf.describe_axis(self.time)

        self.assertEqual(data, self.time_desc)
