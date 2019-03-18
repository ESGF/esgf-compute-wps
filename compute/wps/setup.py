import setuptools

setuptools.setup(
    name='esgf-compute-wps',
    version='devel',
    author='Jason Boutte',
    author_email='boutte3@llnl.gov',
    description='WPS Django Application',
    url='https://github.com/ESGF/esgf-compute-wps',
    packages=setuptools.find_packages('wps'),
)
