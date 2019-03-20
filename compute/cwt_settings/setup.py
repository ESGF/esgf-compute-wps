import os
import setuptools

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setuptools.setup(
    name='cwt_settings',
    version='devel',
    author='Jason Boutte',
    author_email='boutte3@llnl.gov',
    description='ESGF Compute settings',
    url='https://github.com/ESGF/esgf-compute-wps',
    packages=setuptools.find_packages(),
)
