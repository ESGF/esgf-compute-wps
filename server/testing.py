import unittest
test_modules = [ 'kernels']

test_runner = unittest.TextTestRunner(verbosity=2)
for test_module in test_modules:
    test_suite = unittest.defaultTestLoader.discover( './'+test_module )
    test_runner.run(test_suite)

