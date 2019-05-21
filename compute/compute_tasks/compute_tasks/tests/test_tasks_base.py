import unittest

from compute_tasks import base
from compute_tasks import WPSError


class CWTBaseTaskTestCase(unittest.TestCase):
    def test_get_process_missing(self):
        with self.assertRaises(WPSError):
            base.get_process('i do not exist')

    def test_get_process(self):
        @base.register_process('CDAT', 'test', abstract='abstract')
        def test_task(self):
            pass

        proc = base.get_process('CDAT.test')

        self.assertEqual(proc, test_task)
