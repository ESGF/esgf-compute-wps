from wps import settings

from importlib import import_module

from inspect import isclass

from esgf_process import ESGFProcess

import os
import glob

def load_processes():
    process_path = os.path.dirname(__file__)

    candidates = os.listdir(process_path)

    candidates = [os.path.join(process_path, cand) for cand in candidates
                  if os.path.isdir(os.path.join(process_path, cand))]

    processes = []

    for candidate in candidates:
        module_path, _ = os.path.splitext(candidate)
        module_path = os.path.relpath(module_path, settings.BASE_DIR)
        module_path = module_path.replace('/', '.')

        module = import_module(module_path)

        for cls_str in [x for x in module.__dict__ if '__' not in x]:
            cls = getattr(module, cls_str)

            if isclass(cls) and issubclass(cls, ESGFProcess):
                processes.append(cls)

    return processes
