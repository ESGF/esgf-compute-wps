from django import test

class ServerViewsTestCase(test.TestCase):
    def test_home(self):
        response = self.client.get('/wps/')

        self.assertEqual(response.status_code, 200)
