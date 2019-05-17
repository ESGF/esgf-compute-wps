import os
import setuptools

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setuptools.setup(
    name='compute-provisioner',
    version='devel',
    author='Jason Boutte',
    author_email='boutte3@llnl.gov',
    description='Compute Provisioner',
    url='https://github.com/ESGF/esgf-compute-wps',
    packages=['compute_provisioner'],
)
