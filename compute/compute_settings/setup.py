import os
import setuptools

from compute_settings._version import __version__

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setuptools.setup(
    name='compute-settings',
    version=__version__,
    author='Jason Boutte',
    author_email='boutte3@llnl.gov',
    description='ESGF Compute settings',
    url='https://github.com/ESGF/esgf-compute-wps',
    packages=['compute_settings'],
)
