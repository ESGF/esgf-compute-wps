import unittest, sys, os, traceback
from modules.utilities import debug_stop
d = os.path.dirname(__file__)
test_modules = [  'datacache', 'engines', 'kernels', 'request' ]

def print_header( module_name ):
    print>>sys.stderr, "----- "*8
    print>>sys.stderr, "Running unit tests in module '%s' " % test_module
    print>>sys.stderr, "----- "*8

test_runner = unittest.TextTestRunner(verbosity=2)

for test_module in test_modules:
    try:
        print_header( test_module )
        test_suite = unittest.defaultTestLoader.discover( os.path.join( d, test_module ), 'testing.py', d )
        test_runner.run(test_suite)
    except Exception, err:
        print>>sys.stderr, "   ******* Error running unit tests in module '%s' ******* " % test_module
        traceback.print_exc()

debug_stop()

