from django import test

from wps import WPSError
from wps.tasks import base


class CWTBaseTaskTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'servers.json', 'jobs.json']

    def test_get_process_missing(self):
        with self.assertRaises(WPSError):
            base.get_process('i do not exist')

    def test_get_process(self):
        @base.register_process('CDAT', 'test')
        def test_task(self):
            pass

        proc = base.get_process('CDAT.test')

        self.assertEqual(proc, test_task)

    def test_register_process_abstract(self):
        abstract_text = 'CDAT.test abstract text'

        @base.register_process('CDAT', 'test', abstract=abstract_text)
        def test_task(self):
            pass

        self.assertEqual(test_task.IDENTIFIER, 'CDAT.test')
        self.assertEqual(test_task.ABSTRACT, abstract_text)
        self.assertIn('CDAT.test', base.REGISTRY)

    def test_register_process(self):
        @base.register_process('CDAT', 'test')
        def test_task(self):
            pass

        self.assertEqual(test_task.IDENTIFIER, 'CDAT.test')
        self.assertEqual(test_task.ABSTRACT, '')
        self.assertIn('CDAT.test', base.REGISTRY)
