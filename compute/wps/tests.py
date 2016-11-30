import unittest

from django.test import Client

class WPSEndpointTest(unittest.TestCase):
    def setUp(self):
        self.client = Client()

    def test_empty(self):
        response = self.client.get('/wps')

        print response
