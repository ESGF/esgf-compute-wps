import os
import setuptools

from compute_tasks._version import __version__

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setuptools.setup(
    name='compute-tasks',
    version=__version__,
    author='Jason Boutte',
    author_email='boutte3@llnl.gov',
    description='Celery and Dask compute tasks',
    url='https://github.com/ESGF/esgf-compute-wps',
    packages=[
        'compute_tasks',
        'compute_tasks.context',
        'compute_tasks.managers',
        'compute_tasks.templates',
    ],
    entry_points={
        'console_scripts': [
            'compute-tasks-metrics=compute_tasks.metrics_:main',
            'compute-tasks-backend=compute_tasks.backend:main',
            'compute-tasks-register=compute_tasks.backend:register_processes',
        ],
    },
    package_data={
        '': ['*.yaml'],
    },
)
