import os
import setuptools

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setuptools.setup(
    name='esgf-compute-wps',
    version='devel',
    author='Jason Boutte',
    author_email='boutte3@llnl.gov',
    description='WPS Django Application',
    url='https://github.com/ESGF/esgf-compute-wps',
    packages=[
        'wps',
        'wps.auth',
        'wps.backends',
        'wps.management',
        'wps.migrations',
        'wps.tasks',
        'wps.templates',
        'wps.util',
        'wps.views',
    ],
)
