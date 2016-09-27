import os
import glob

from django.conf import settings

def file_path_to_package(path):
    path_no_ext, _ = os.path.splitext(path)

    package_path = os.path.relpath(path_no_ext, settings.BASE_DIR)

    return package_path.replace('/', '.')

def load_processes():
    process_path = os.path.dirname(__file__)

    process_file_paths = glob.glob(os.path.join(process_path, '**/*.py'))

    return [file_path_to_package(x) for x in process_file_paths
            if '__init__' not in x]

import sys

# After Pywps forks for async operation the sys.path does not include
# the base path to the django project. Imports from the root package i.e.
# from wps import settings will not work until path is fixed. Oddly 
# the path for ./compute/wps is in sys.path and we can load settings through
# import settings.
if settings.BASE_DIR not in sys.path:
    sys.path.insert(2, settings.BASE_DIR)

__all__ = load_processes()
