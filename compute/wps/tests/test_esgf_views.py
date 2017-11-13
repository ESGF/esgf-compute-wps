import json
import os

import cdms2
import mock
import numpy as np
from django import test
from django.core.cache import cache

from wps import models
from wps.views import esgf

class ESGFViewsTestCase(test.TestCase):
    fixtures = ['users.json']

    def setUp(self):
        self.user = models.User.objects.all()[0]

        self.solr_empty = os.path.join(os.path.dirname(__file__), 'data', 'solr_empty.json')

        self.solr_full = os.path.join(os.path.dirname(__file__), 'data', 'solr_full.json')

        cache.clear()

    @mock.patch('cdms2.open')
    @mock.patch('wps.views.esgf.process.CWTBaseTask')
    def test_retrieve_axes(self, mock_task, mock_open):
        axes = [
            cdms2.createAxis(np.array([x for x in xrange(10)])),
            cdms2.createAxis(np.array([x for x in xrange(10)])),
            cdms2.createAxis(np.array([x for x in xrange(10)])),
            cdms2.createAxis(np.array([x for x in xrange(10)])),
        ]

        axes[0].id = 'time'
        axes[0].designateTime()
        axes[0].units = 'days since 1990-1-1'

        axes[1].id = 'lat'
        axes[1].designateLatitude()
        axes[1].units = 'degrees south'

        axes[2].id = 'lon'
        axes[2].designateLongitude()
        axes[2].units = 'degrees west'

        axes[3].id = 'lev'
        axes[3].designateLevel()
        axes[3].units = 'm'

        mock_open.return_value.__enter__.return_value.__getitem__.return_value.getAxisList.return_value = axes

        old_cwd = os.getcwd()

        result = esgf.retrieve_axes(self.user, 'dataset_id', 'tas', ['file:///hello1.nc'])

        self.assertEqual(result.keys(), ['lat', 'lon', 'lev', 'time'])

        os.chdir(old_cwd)

    def test_retrieve_axes_access_error(self):
        old_cwd = os.getcwd()

        with self.assertRaises(Exception):
            esgf.retrieve_axes(self.user, 'dataset_id', 'tas', ['file:///hello1.nc'])

        os.chdir(old_cwd)

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
        #esgf.search_solr('cmip5.output1.MPI-M.MPI-ESM-LR.abrupt4xCO2.mon.atmos.Amon.r1i1p1.v20120602|esgf1.dkrz.de', 'esgf-data.dkrz.de')

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

    @mock.patch('requests.get')
    def test_search_dataset_auth(self, mock_get):
        with open(self.solr_empty) as infile:
            data = infile.read()

        mock_get.return_value.content = data

        self.client.login(username=self.user.username, password=self.user.username)

        params = {
            'dataset_id': 'dataset_id',
            'index_node': 'index_node',
        }

        response = self.client.get('/wps/search/', params)

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'No variables were found in dataset "dataset_id"')

    def test_search_dataset_missing_dataset(self):
        self.client.login(username=self.user.username, password=self.user.username)

        response = self.client.get('/wps/search/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Missing parameter "\'dataset_id\'"')

    def test_search_dataset(self):
        response = self.client.get('/wps/search/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')
