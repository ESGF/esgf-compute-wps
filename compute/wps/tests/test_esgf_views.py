import json
import os

import cdms2
import mock
import numpy as np
from django import test
from django.core.cache import cache

from . import helpers
from wps import models
from wps import WPSError
from wps.views import esgf

class ESGFViewsTestCase(test.TestCase):
    fixtures = ['users.json']

    def setUp(self):
        self.user = models.User.objects.all()[0]

        self.solr_empty = os.path.join(os.path.dirname(__file__), 'data', 'solr_empty.json')

        self.solr_full = os.path.join(os.path.dirname(__file__), 'data', 'solr_full.json')

        cache.clear()

        self.axes = [
            cdms2.createAxis(np.array([x for x in xrange(10)])),
            cdms2.createAxis(np.array([x for x in xrange(10)])),
            cdms2.createAxis(np.array([x for x in xrange(10)])),
            cdms2.createAxis(np.array([x for x in xrange(10)])),
        ]

        self.axes[0].id = 'time'
        self.axes[0].designateTime()
        self.axes[0].units = 'days since 1990-1-1'

        self.axes[1].id = 'lat'
        self.axes[1].designateLatitude()
        self.axes[1].units = 'degrees south'

        self.axes[2].id = 'lon'
        self.axes[2].designateLongitude()
        self.axes[2].units = 'degrees west'

        self.axes[3].id = 'lev'
        self.axes[3].designateLevel()
        self.axes[3].units = 'm'

    @mock.patch('wps.views.esgf.tasks.load_certificate')
    @mock.patch('wps.views.esgf.cdms2.open')
    def test_retrieve_axes_multiple_files(self, mock_open, mock_load):
        axis1 = mock.MagicMock()
        axis1.__enter__.return_value.__getitem__.return_value.getAxisList.return_value = self.axes
        axis2 = mock.MagicMock()
        axis2.__enter__.return_value.__getitem__.return_value.getAxisList.return_value = []

        mock_open.side_effect = [axis1, axis2]

        result = esgf.retrieve_axes(self.user, 'dataset_id', 'tas', ['file:///hello1.nc', 'file:///hello2.nc'])

        self.assertEqual(result.keys(), ['lat', 'lon', 'lev', 'time'])

    @mock.patch('wps.views.esgf.tasks.load_certificate')
    @mock.patch('wps.views.esgf.cdms2.open')
    def test_retrieve_axes(self, mock_open, mock_load):
        mock_open.return_value.__enter__.return_value.__getitem__.return_value.getAxisList.return_value = self.axes

        result = esgf.retrieve_axes(self.user, 'dataset_id', 'tas', ['file:///hello1.nc'])

        self.assertEqual(result.keys(), ['lat', 'lon', 'lev', 'time'])

    def test_retrieve_axes_no_files(self):
        with self.assertRaises(WPSError) as e:
            esgf.retrieve_axes(self.user, 'dataset_id_not_in_cache', 'tas', [])

    @mock.patch('wps.views.esgf.tasks.load_certificate')
    def test_retrieve_axes_access_error(self, mock_load):
        with self.assertRaises(Exception):
            esgf.retrieve_axes(self.user, 'dataset_id', 'tas', ['file:///hello1.nc'])

    def test_parse_solr_docs_data(self):
        with open(self.solr_full) as infile:
            data = json.load(infile)

        variables = esgf.parse_solr_docs(data['response']['docs'])

        self.assertEqual(len(variables.keys()), 50)

        for var in variables.keys():
            self.assertIn('files', variables[var])

    def test_parse_solr_docs(self):
        with open(self.solr_empty) as infile:
            data = json.load(infile)

        variables = esgf.parse_solr_docs(data['response']['docs'])

        self.assertEqual(len(variables.keys()), 0)

    @mock.patch('requests.get')
    def test_search_solr_malformed_json(self, mock_get):
        mock_get.return_value.content = 42

        with self.assertRaises(Exception):
            esgf.search_solr('dataset_id', 'index_node')

    def test_search_solr_network_error(self):
        with self.assertRaises(Exception):
            esgf.search_solr('dataset_id', 'index_node')

    @mock.patch('requests.get')
    def test_search_solr_data(self, mock_get):
        with open(self.solr_full) as infile:
            data = infile.read()

        mock_get.return_value.content = data

        result = esgf.search_solr('dataset_id', 'index_node')

        self.assertEqual(len(result), 100)

    @mock.patch('requests.get')
    def test_search_solr(self, mock_get):
        with open(self.solr_empty) as infile:
            data = infile.read()

        mock_get.return_value.content = data

        result = esgf.search_solr('dataset_id', 'index_node')

        self.assertEqual(len(result), 0)

    def test_search_variable_missing_authentication(self):
        response = self.client.get('/wps/search/variable', {})

        helpers.check_failed(self, response)

    def test_search_variable_missing_parameter(self):
        self.client.login(username=self.user.username, password=self.user.username)

        response = self.client.get('/wps/search/variable', {})

        helpers.check_failed(self, response)

    @mock.patch('wps.views.esgf.retrieve_axes')
    @mock.patch('wps.views.esgf.parse_solr_docs')
    @mock.patch('wps.views.esgf.search_solr')
    def test_search_variable(self, mock_search, mock_parser, mock_retrieve):
        mock_retrieve.return_value = {}

        self.client.login(username=self.user.username, password=self.user.username)

        response = self.client.get('/wps/search/variable', {'dataset_id': 'dataset_id', 'index_node': 'index_node', 'variable': 'tas'})

        helpers.check_success(self, response)

    @mock.patch('requests.get')
    def test_search_dataset_no_variables(self, mock_get):
        with open(self.solr_empty) as infile:
            data = infile.read()

        mock_get.return_value.content = data

        self.client.login(username=self.user.username, password=self.user.username)

        params = {
            'dataset_id': 'dataset_id',
            'index_node': 'index_node',
        }

        response = self.client.get('/wps/search/', params)

        helpers.check_failed(self, response)

    def test_search_dataset_missing_dataset(self):
        self.client.login(username=self.user.username, password=self.user.username)

        response = self.client.get('/wps/search/')

        helpers.check_failed(self, response)

    def test_search_dataset_missing_authentication(self):
        response = self.client.get('/wps/search/')

        helpers.check_failed(self, response)
