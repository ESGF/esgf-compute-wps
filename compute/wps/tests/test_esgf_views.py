from .common import CommonTestCase

class ESGFViewsTestCase(CommonTestCase):

    def test_esgf_search_auth(self):
        self.client.login(username='test', password='test')

        params = {
            'dataset_id': 'tas',
            'index_node': 'esgf-node.llnl.gov',
        }

        response = self.client.get('/wps/search/', params)

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Failed to retrieve search results')

    def test_esgf_search_missing_dataset(self):
        self.client.login(username='test', password='test')

        response = self.client.get('/wps/search/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], {u'message': u'Mising required parameter "\'dataset_id\'"'})

    def test_esgf_search(self):
        response = self.client.get('/wps/search/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')
