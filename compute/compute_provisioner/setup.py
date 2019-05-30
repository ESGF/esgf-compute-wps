import os
import setuptools

from compute_provisioner._version import __version__

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setuptools.setup(
    name='compute-provisioner',
    version=__version__,
    author='Jason Boutte',
    author_email='boutte3@llnl.gov',
    description='Compute Provisioner',
    url='https://github.com/ESGF/esgf-compute-wps',
    packages=['compute_provisioner'],
)
