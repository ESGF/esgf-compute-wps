import os
import setuptools

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setuptools.setup(
    name='compute-kubernetes',
    version='devel',
    packages=['compute_kubernetes', ],
    entry_points={
        'console_scripts': [
            'compute-kube-monitor=compute_kubernetes:main',
        ],
    }
)
