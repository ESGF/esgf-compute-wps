import os
import glob
import types
import traceback

from datetime import datetime

from importlib import import_module

from wps import logger
from wps.processes.esgf_process import ESGFProcess
from wps.processes.esgf_operation import ESGFOperation

from django.conf import settings

def _file_path_to_module_name(module_path, base_path):
    rel_path = os.path.relpath(module_path, base_path)

    module, _ = os.path.splitext(rel_path)

    return module.replace('/', '.')

def _load_operations_from_module(module_name):
    operations = []

    try:
        module = import_module(module_name)
    except Exception as e:
        logger.exception('Failed to import module %s', module_name)

        return None
    else:
        for sub_module_name in dir(module):
            sub_module = getattr(module, sub_module_name)

            if type(sub_module) == types.TypeType:
                if (issubclass(sub_module, ESGFOperation) and
                        sub_module != ESGFOperation):
                    try:
                        operations.append(sub_module())
                    except Exception as e:
                        logger.exception('Failed to create instance of '
                                         'module "%s', sub_module_name)

    return operations

def _load_processes():
    processes = []

    candidates = glob.glob(os.path.join(os.path.dirname(__file__), '**/*.py'))

    candidates = [x for x in candidates if '__init__.py' not in x]

    for candidate in candidates:
        logger.info('Checking %s for ESGFOperations', candidate)

        module_name = _file_path_to_module_name(candidate, settings.BASE_DIR)

        operations = _load_operations_from_module(module_name)

        if operations:
            for operation in operations:
                try:
                    processes.append(ESGFProcess(operation))
                except Exception:
                    logger.exception('Failed to create process for operation'
                                     '"%s"', operation.identifier)

    return processes

start = datetime.now()

try:
    PROCESSES = _load_processes()
except Exception as e:
    logger.exception('Error loading processes: %s', e.message)

logger.info('Finished loading processes in %s', datetime.now()-start)

__all__ = []
