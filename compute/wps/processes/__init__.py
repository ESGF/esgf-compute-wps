from wps import settings

from importlib import import_module

from inspect import isclass

from esgf_process import ESGFProcess

import os
import glob

def file_path_to_package(path):
    path_no_ext, _ = os.path.splitext(path)

    package_path = os.path.relpath(path_no_ext, settings.BASE_DIR)

    return package_path.replace('/', '.')

def load_processes():
    process_path = os.path.dirname(__file__)

    process_file_paths = glob.glob(os.path.join(process_path, '**/*.py'))

    return [file_path_to_package(x) for x in process_file_paths
            if '__init__' not in x]

__all__ = load_processes()
