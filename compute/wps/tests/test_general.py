#! /usr/bin/env python

from django import test

class GeneralTestCase(test.TestCase):

    def test_static(self):
        response = self.client.get('/static')

        self.assertEqual(response.status_code, 404)
