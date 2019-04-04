import os
import setuptools

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setuptools.setup(
    name='cwt-kubernetes',
    version='devel',
    packages=['cwt_kubernetes', ],
    entry_points={
        'console_scripts': [
            'cwt-kube-monitor=cwt_kubernetes:main',
        ],
    }
)
